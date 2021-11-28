import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import psycopg2
import sqlalchemy
from sqlalchemy import exc
from requests.exceptions import ReadTimeout
import random
import time


def fill_genres_table(connection):
    my_genres = ['indie', 'hip-hop', 'jazz', 'rock', 'pop', 'empty']
    for genre in my_genres:
        try:
            connection.execute(f'''INSERT INTO genres(name) VALUES('{genre}')''')
        except exc.IntegrityError:
            print('такой жанр уже существует')
    print('Жанры заполнены')


def find_genre_id(connection, sp, genre_name):
    return connection.execute("SELECT genre_id FROM genres WHERE name='" + genre_name + "' ").fetchone()[0]


def fill_artists(connection, sp, data=None):
    if data != None:
        try:
            connection.execute(
                f"INSERT INTO artists(sp_id, pseudonym) VALUES('{data['sp_id']}', '{data['pseudonym']}')")
            artist_id = connection.execute("SELECT id FROM artists WHERE sp_id='" + data['sp_id'] + "' ").fetchone()[0]
            connection.execute(
                f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES('{artist_id}', '{data['genre_id']}')''')
        except sqlalchemy.exc.IntegrityError:
            print(f'Артист уже есть в базе данных')
        finally:
            return artist_id

    genres_list = connection.execute('''SELECT * FROM genres''').fetchall()
    last_id = None
    for genre in genres_list:
        genre_id = find_genre_id(connection, sp, genre[1])
        res = sp.recommendations(seed_genres=[genre[1]])
        for i in res['tracks']:
            for j in i['artists']:
                # print(j['name']+'======' + j['id'])
                sp_id = j['id']
                sp_name = j['name'].replace("'", "''")
                try:
                    connection.execute(f'''INSERT INTO artists(sp_id, pseudonym) VALUES('{sp_id}', '{sp_name}')''')
                    # print(resp.lastrowid)
                    artist_id = connection.execute("SELECT id FROM artists WHERE sp_id='" + sp_id + "' ").fetchone()[0]
                    connection.execute(
                        f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES('{artist_id}', '{genre_id}')''')
                except sqlalchemy.exc.IntegrityError:
                    print(f'Артист {j["name"]} уже есть в базе данных')

    print('Артисты заполнены')


def fill_albums(connection, sp):
    artists_list = connection.execute('''SELECT * FROM artists''').fetchall()
    for artist in artists_list:
        sp.requests_timeout = 1
        albums = sp.artist_albums(artist[1], album_type='album', limit=5, offset=0)
        for album in albums['items']:
            sp_id = album['id']
            al_year = album['release_date'][0:4]
            al_name = album['name'].replace("'", "''").replace("%", "%%")
            try:
                connection.execute(
                    f'''INSERT INTO albums(album_name, album_year, sp_id) VALUES('{al_name}', {al_year}, '{sp_id}')''')

            except sqlalchemy.exc.IntegrityError:
                print(f'Альбом {al_name} уже есть в базе данных')

    print('Альбомы заполнены')


def fill_album_artists(connection, sp):
    print('...Заполняется отношение альбомов и артистов...')
    album_list = connection.execute('''SELECT * FROM albums''').fetchall()
    for album in album_list:
        # print(album[1] + "============" + album[2])
        try:
            album_artists = sp.album(album[2])['artists']
        except ReadTimeout:
            print('Spotify timed out... trying again...')
            album_artists = sp.album(album[2])['artists']

        for al_artist in album_artists:
            artist_sp_id = al_artist['id']
            artist_data = connection.execute(
                "SELECT id FROM artists WHERE sp_id = '" + al_artist['id'] + "'").fetchone()
            if artist_data == None:
                artist_info = {'sp_id': '', 'pseudonym': '', 'genre': ''}
                artist_info['sp_id'] = artist_sp_id
                artist_info['pseudonym'] = al_artist['name'].replace("'", "''")
                # exists_artist_id = connection.execute(f'SELECT artist_id FROM albums_artists WHERE album_id = {album[0]}').fetchone()
                # genre_id = connection.execute(
                #     f'SELECT genre_id FROM Artists_by_genres WHERE artist_id = {exists_artist_id[0]}').fetchone()
                artist_spid_for_genre = sp.album(album[2])['artists'][0]['id']
                genre_name_list = sp.artist(artist_spid_for_genre)['genres']
                if len(genre_name_list) == 0:
                    genre_name = 'empty'
                else:
                    genre_name = genre_name_list[0].replace("'", "''")

                existed_genre = connection.execute(
                    f"SELECT genre_id FROM genres WHERE name = '{genre_name}'").fetchone()

                if existed_genre == None:
                    connection.execute(f'''INSERT INTO genres(name) VALUES('{genre_name}')''')
                    artist_info['genre_id'] = connection.execute(
                        "SELECT genre_id FROM genres WHERE name = '" + genre_name + "'").fetchone()

                else:
                    artist_info['genre_id'] = existed_genre[0]

                # artist_info['genre_id'] = sp.artist(artist_sp_id)['genres'][0]
                new_artist_id = fill_artists(connection, sp, artist_info)
                if new_artist_id == None:
                    continue

                try:
                    connection.execute(
                        f'''INSERT INTO albums_artists(album_id, artist_id) VALUES('{album[0]}', '{new_artist_id}') ''')
                except sqlalchemy.exc.IntegrityError:
                    print('такая запись уже имеется в таблице')

            else:
                artist_id = artist_data[0]

                try:
                    connection.execute(
                        f'''INSERT INTO albums_artists(album_id, artist_id) VALUES('{album[0]}', '{artist_id}') ''')
                except sqlalchemy.exc.IntegrityError:
                    print('такая запись уже имеется в таблице')

    print('Отношение альбомов и артистов заполнено')


def fill_tracks(connection, sp):
    print('...Заполняется список треков...')
    album_list = connection.execute('''SELECT album_id, sp_id FROM albums''').fetchall()
    for album in album_list:
        track_list = sp.album(album[1])['tracks']['items']
        for track in track_list:
            # track_data = {'sp_id':'','track_name':'','track_duration':'','album_id':''}
            sp_id = track['id']
            track_name = track['name'].replace("'", "''").replace("%", "%%")
            track_duration = int(track['duration_ms'] / 1000)
            album_id = album[0]
            try:
                connection.execute(
                    f'''INSERT INTO tracks(sp_id, track_name, track_duration, album_id) VALUES('{sp_id}', '{track_name}', {track_duration}, {album_id})''')
            except sqlalchemy.exc.IntegrityError:
                print('такая запись уже имеется в таблице')

    print('Треки заполнены')


def create_my_compilation(connection, sp):
    compilation_list = list(range(1, random.randint(7, 10)))
    # compilation_in_base = conne
    for i in compilation_list:
        last_exist_id = connection.execute("SELECT ca_id FROM Compilation_albums ORDER BY ca_id DESC").fetchone()
        if last_exist_id == None:
            last_ca_id = 1
        else:
            last_ca_id = last_exist_id[0] + 1

        connection.execute(
            f'''INSERT INTO Compilation_albums(ca_name, ca_year) VALUES('My compilation №{last_ca_id}',{random.randint(2010, 2022)})''')

    print('Сборники созданы')


def fill_compilation_albums(connection, sp):
    tracks_list_in_base = connection.execute("SELECT track_id FROM tracks").fetchall()
    tracks_count_in_base = len(tracks_list_in_base)
    tracks_count_in_compilation = 20
    c_albums = connection.execute("SELECT ca_id FROM Compilation_albums").fetchall()
    index_list = random.sample(range(1, tracks_count_in_base + 1), 20)

    for ca in c_albums:
        for random_track_id in random.sample(tracks_list_in_base, 20):
            connection.execute(
                f'''INSERT INTO Compilation_album_tracks(ca_id, track_id) VALUES({ca[0]},{random_track_id[0]})''')

    print('Сборники заполнены')


def do_action(connection, sp, action):
    if action == '1':
        fill_genres_table(connection)
    elif action == '2':
        fill_artists(connection, sp)
    elif action == '3':
        fill_albums(connection, sp)
    elif action == '4':
        fill_album_artists(connection, sp)
    elif action == '5':
        fill_tracks(sp)
    elif action == '6':
        create_my_compilation(connection, sp)
    elif action == '7':
        fill_compilation_albums(connection, sp)
    else:
        print('Выберите другую команду')


def fill_data_for_testing_hw(connection, sp):
    try:
        connection.execute(
            f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES(1, 2)''')
    except sqlalchemy.exc.IntegrityError:
        connection.execute(
            f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES(1, 3)''')

    try:
        connection.execute(
            f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES(2, 4)''')
    except sqlalchemy.exc.IntegrityError:
        connection.execute(
            f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES(2, 1)''')

    try:
        connection.execute(
            f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES(3, 2)''')
    except sqlalchemy.exc.IntegrityError:
        connection.execute(
            f'''INSERT INTO artists_by_genres(artist_id, genre_id) VALUES(3, 3)''')


def fill_the_base(connection, sp):
    fill_genres_table(connection)
    time.sleep(1)
    fill_artists(connection, sp)
    time.sleep(1)
    fill_albums(connection, sp)
    time.sleep(1)
    fill_album_artists(connection, sp)
    time.sleep(3)
    fill_tracks(connection, sp)
    create_my_compilation(connection, sp)
    fill_compilation_albums(connection, sp)
    fill_data_for_testing_hw(connection, sp)


if __name__ == '__main__':
    APP_ID = '7f7ac36820a24e6587bf114e48054cb2'
    APP_SECRET = '5dbdb56d47b54cd68665291ba3a909c2'
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=APP_ID, client_secret=APP_SECRET),
                         requests_timeout=15)

    # 'postgresql://postgres:Gunner90@localhost:5432/SQL_HomeWork_3'

    db_string = input('Введите строку подключения к базе данных')
    db = db_string
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()

    # action = input('Выберите команду: ')
    # do_action(sp, action)
    fill_the_base(connection, sp)
    print('done!!!')
