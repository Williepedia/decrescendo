import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import configparser

def spotify_release_date(query):
    config = configparser.ConfigParser()
    config.read('config.ini')

    if isinstance(query,list):
        for i in range(0, len(query)):
            if i == 0:
                results = spotify_release_date(query[i])
            else:
                results.extend(spotify_release_date(query[i]))
            i+=1
    else:
        client_credentials_manager = SpotifyClientCredentials(
            client_id=config['spotify_credentials']['client_id'],
            client_secret=config['spotify_credentials']['client_secret'],
        )
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        result = sp.search(query, limit=1)
        results = []
        for item in result['tracks']['items']:
            if item['album']['release_date_precision'] == 'day':
                track_data = sp.track(item['id'])
                results.append({'query': query,
                                'spotify_id': item['id'],
                                'popularity' : track_data['popularity'],
                                'song': item['name'],
                                'album':item['album']['name'],
                                'album_type':item['album']['type'],
                                'artist': item['album']['artists'][0]['name'],
                                'release_date': item['album']['release_date']})
    return results
