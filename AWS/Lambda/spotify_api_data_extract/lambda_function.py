import json
from datetime import datetime
import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Initialize Spotify client with credentials
client_credentials_manager = SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Initialize AWS S3 client
client = boto3.client('s3')

def lambda_handler(event, context):
    """AWS Lambda handler function to extract Spotify playlist data and push it to S3."""
    playlist_links = [
        "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M",
        "https://open.spotify.com/playlist/37i9dQZF1DX0ieekvzt1Ic",
        "https://open.spotify.com/playlist/37i9dQZF1DWY4lFlS4Pnso",
        "https://open.spotify.com/playlist/37i9dQZF1DX0kbJZpiYdZl"
    ]
    playlist_names = [
        "TopHits", "HotHitsIndia", "HotHitsUK", "HotHitsUSA"
    ]

    # Dictionary to store playlist data
    spotify_data = {}

    for i, link in enumerate(playlist_links):
        playlist_uri = link.split('/')[-1]
        playlist_data = sp.playlist_tracks(playlist_uri)
        spotify_data[playlist_names[i]] = playlist_data

    # Generate filename with timestamp
    filename = f"spotify_raw_{datetime.now().isoformat()}.json"

    # Extract step: Push unstructured data to S3
    client.put_object(
        Bucket="spotify-snowflake-etl-pavan",
        Key=f"raw_data/to_process/{filename}",
        Body=json.dumps(spotify_data)
    )