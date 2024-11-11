import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
import os
from datetime import datetime

#Define these instances outside, since these will be initialized only the first time. During the Cold Start.
#This will speed up the performance.
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
client = boto3.client('s3')

def lambda_handler(event, context):
    playlist_links = ["https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M", "https://open.spotify.com/playlist/37i9dQZF1DX0ieekvzt1Ic",
        "https://open.spotify.com/playlist/37i9dQZF1DWY4lFlS4Pnso", "https://open.spotify.com/playlist/37i9dQZF1DX0kbJZpiYdZl"]
    playlist_names = ["TopHits", "HotHitsIndia", "HotHitsUK", "HotHitsUSA"]

    for i in range(len(playlist_links)):
        playlist_URI = playlist_links[i].split('/')[-1]
        spotify_data = sp.playlist_tracks(playlist_URI)
        #print(spotify_data)
        filename = "spotify_raw_" + playlist_names[i] + "_" + str(datetime.now()) + ".json"
        print(filename)

        #This is the Extract part -> Lambda pushes the unstructured data to S3
        client.put_object(
            Bucket="spotify-snowflake-etl-pavan",
            Key="raw_data/to_process/" + filename,
            Body=json.dumps(spotify_data)
        )