# This is a sample of an ETL Python script from spotify.
# _*_ encoding: utf-8 _*_

import spotipy
import calendar
import time
import datetime
from spotipy.oauth2 import SpotifyClientCredentials
from pymongo import MongoClient

# artists ids [led_zeppelin, metallica, black sabbath, iron_maiden, scorpions]
artists = ['36QJpDe2go2KgaRleHCDTp', '2ye2Wgw4gimLv2eAKyk1NB', '5M52tdBnJaKSvOpJGz8mfZ', '6mdiAmATAx73kdxrNrnlao',
           '27T030eWyCQRmDyuvr1kxY']

# establish connection to spotify api
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="615f207a1b824e7592e350cad109500b",
                                                           client_secret="653f9b1a4ffe402ab512f13328b3a733"))

# print(connection.list_database_names())

# extract the info related to artists
for artist_id in artists:

    # establish connection to mongodb
    connection = MongoClient('localhost', 27017)

    # create database
    db = connection['spotify']

    # generate collections
    artist_collection = db['artists']
    album_collection = db['albums']
    track_collection = db['tracks']

    # extract info's artist from spotify
    artist = sp.artist('spotify:artist:' + artist_id)

    artist_name = artist['name']
    artist_popularity = artist['popularity']
    artist_type = artist['type']
    artist_uri = artist['uri']
    artist_followers = artist['followers']
    artist_genres = artist['genres']
    artist_source = artist['href']
    now = calendar.timegm(time.gmtime())

    # save artist info in a mongodb collection (artists)
    try:
        artist_collection.insert_one({
            '_id': artist_id,
            'nombre': artist_name,
            'popularidad': artist_popularity,
            'tipo': artist_type,
            'uri': artist_uri,
            'seguidores': artist_followers['total'],
            'generos': artist_genres,
            'origen': artist_source,
            'fecha_carga': now
        })

    except:
        print('')

    # print artist's info from spotify
    finally:
        print(' ')
        print('nombre :         ' + artist_name)
        print('popularidad :    ' + str(artist_popularity))
        print('tipo :           ' + artist_type)
        print('uri :            ' + artist_uri)
        print('seguidores :     ' + str(artist_followers['total']))
        print('generos:         ' + str(artist_genres))
        print('origen :         ' + artist_source)
        print('current time:    ' + datetime.datetime.fromtimestamp(now).strftime(
            '%Y-%m-%d %H:%M:%S'))

    # extract info of artist's albums from spotify
    artist_albums = sp.artist_albums('spotify:artist:' + artist_id, album_type='album', limit=50)

    artist_albums_filtered = []
    artist_album_name = []

    # filter artist's albums considering the name of album to avoid repeat info
    for album in artist_albums['items']:
        if album['name'] not in artist_album_name:
            artist_album_name.append(album['name'])
            artist_albums_filtered.append(album)

    # seek and extract info related to an album
    for filtered in artist_albums_filtered:

        # extract info's album from spotify
        album = sp.album(filtered['id'])

        album_id = album['id']
        album_name = album['name']
        album_type = album['album_type']
        album_artist_info = album['artists'][0]
        album_artist = album_artist_info['name']
        album_release = album['release_date']
        album_total_tracks = album['total_tracks']
        album_popularity = album['popularity']
        album_uri = album['uri']
        album_source = album['href']
        now = calendar.timegm(time.gmtime())

        # save album info in a mongodb collection (albums)
        try:
            album_collection.insert_one({
                '_id': album_id,
                'nombre': album_name,
                'tipo': album_type,
                'artista': album_artist,
                'lanzamiento': album_release,
                'numero de pistas': album_total_tracks,
                'popularidad': album_popularity,
                'uri': album_uri,
                'origen': album_source,
                'fecha_carga': now
            })

        except:
            print('\t|')

        # print album's info from spotify
        finally:
            if filtered != artist_albums_filtered[len(artist_albums_filtered) - 1]:
                print('\t|')
                print('\t|---album        : ' + album_name + ',\tlanzamiento : ' + album_release +
                      ',\tnumero de pistas : ' + str(album_total_tracks))
                print('\t|   popularidad  : ' + str(album_popularity))
                print('\t|   origen       : ' + album_source)
                print('\t|   current time : ' + datetime.datetime.fromtimestamp(now).strftime(
                    '%Y-%m-%d %H:%M:%S'))
            else:
                print('\t|')
                print('\t|---album        : ' + album_name + ',\tlanzamiento : ' + album_release +
                      ',\tnumero de pistas : ' + str(album_total_tracks))
                print('\t    popularidad  : ' + str(album_popularity))
                print('\t    origen       : ' + album_source)
                print('\t    current time : ' + datetime.datetime.fromtimestamp(now).strftime(
                    '%Y-%m-%d %H:%M:%S'))

        # extract info of track's album from spotify
        album_tracks = sp.album_tracks(album['id'])
        track_items = album_tracks['items']

        # seek and extract info related to a track
        for track in track_items:

            # extract info's track from spotify
            track_info = sp.track(track['id'])

            track_id = track_info['id']
            track_name = track_info['name']
            track_type = track_info['type']
            track_artist_info = track_info['artists']
            track_artist = track_artist_info[0]['name']
            track_album_info = track_info['album']
            track_album = track_album_info['name']
            track_number = track_info['track_number']
            track_popularity = track_info['popularity']
            track_release = track_album_info['release_date']
            track_uri = track_info['uri']
            track_source = track['href']
            now = calendar.timegm(time.gmtime())

            # save track info in a mongodb collection (albums)
            try:
                track_collection.insert_one({
                    '_id': track_id,
                    'nombre': track_name,
                    'tipo': track_type,
                    'artista': track_artist,
                    'album': track_album,
                    'numero de pista': track_number,
                    'lanzamiento': track_release,
                    'popularidad': track_popularity,
                    'uri': track_uri,
                    'origen': track_source,
                    'fecha_carga': now
                })

            except:
                if filtered == artist_albums_filtered[len(artist_albums_filtered) - 1]:
                    print('\t\t|')
                else:
                    print('\t|\t|')

            # print track's info from spotify
            finally:
                if filtered != artist_albums_filtered[len(artist_albums_filtered) - 1]:
                    if track != track_items[len(track_items) - 1]:
                        print('\t|\t|---track        : ' + track_name + ', \tlanzamiento : ' + track_release +
                              ', \tnumero de pista   : ' + str(track_number))
                        print('\t|\t|   popularidad  : ' + str(track_popularity))
                        print('\t|\t|   origen       : ' + track_source)
                        print('\t|\t|   current time : ' + datetime.datetime.fromtimestamp(
                            now).strftime(
                            '%Y-%m-%d %H:%M:%S'))
                    else:
                        print('\t|\t|---track        : ' + track_name + ', \tlanzamiento : ' + track_release +
                              ', \tnumero de pista   : ' + str(track_number))
                        print('\t|\t    popularidad  : ' + str(track_popularity))
                        print('\t|\t    origen       : ' + track_source)
                        print('\t|\t    current time : ' + datetime.datetime.fromtimestamp(
                            now).strftime(
                            '%Y-%m-%d %H:%M:%S'))
                else:
                    if track != track_items[len(track_items) - 1]:
                        print('\t\t|---track        : ' + track_name + ', \tlanzamiento : ' + track_release +
                              ', \tnumero de pista   : ' + str(track_number))
                        print('\t\t|    popularidad  : ' + str(track_popularity))
                        print('\t\t|    origen       : ' + track_source)
                        print('\t\t|    current time : ' + datetime.datetime.fromtimestamp(
                            now).strftime(
                            '%Y-%m-%d %H:%M:%S'))
                    else:
                        print('\t\t|---track        : ' + track_name + ', \tlanzamiento : ' + track_release +
                              ', \tnumero de pista   : ' + str(track_number))
                        print('\t\t    popularidad  : ' + str(track_popularity))
                        print('\t\t    origen       : ' + track_source)
                        print('\t\t    current time : ' + datetime.datetime.fromtimestamp(
                            now).strftime(
                            '%Y-%m-%d %H:%M:%S'))


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
