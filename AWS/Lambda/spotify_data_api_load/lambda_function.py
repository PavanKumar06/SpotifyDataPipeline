import json
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO

# Low-level API: Provides more control
client = boto3.client('s3')
BUCKET = 'spotify-snowflake-etl-pavan'
KEY = 'raw_data/to_process/'
song_buffer = StringIO()
album_buffer = StringIO()
artist_buffer = StringIO()

# High-level API
resource = boto3.resource('s3')


def get_album_list(data):
    """Extracts album data from Spotify playlist."""
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {
            'album_id': album_id,
            'name': album_name,
            'release_date': album_release_date,
            'total_tracks': album_total_tracks,
            'url': album_url
        }
        album_list.append(album_element)
    return album_list


def get_artist_list(data):
    """Extracts artist data from Spotify playlist."""
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'external_url': artist['href']
                    }
                    artist_list.append(artist_dict)
    return artist_list


def get_song_list(data):
    """Extracts song data from Spotify playlist."""
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'duration_ms': song_duration,
            'url': song_url,
            'popularity': song_popularity,
            'song_added': song_added,
            'album_id': album_id,
            'artist_id': artist_id
        }
        song_list.append(song_element)
    return song_list


def parse_release_date(date):
    """Parses and standardizes album/song release dates."""
    try:
        if len(date) == 4:  # Year only
            return pd.to_datetime(date + "-01-01")
        return pd.to_datetime(date)
    except Exception:
        return pd.NaT


def lambda_handler(event, context):
    """Main handler to process Spotify playlist data and upload to S3."""
    file_key = client.list_objects(Bucket=BUCKET, Prefix=KEY)['Contents'][0]['Key']
    response = client.get_object(Bucket=BUCKET, Key=file_key)
    content = response['Body']
    spotify_data = json.loads(content.read())
    spotify_file_key = file_key

    # Combine data from all playlists
    combined_album_list = []
    combined_artist_list = []
    combined_song_list = []

    for playlist_name, playlist_data in spotify_data.items():
        combined_album_list.extend(get_album_list(playlist_data))
        combined_artist_list.extend(get_artist_list(playlist_data))
        combined_song_list.extend(get_song_list(playlist_data))

    album_df = pd.DataFrame.from_dict(combined_album_list).drop_duplicates(subset=['album_id'])
    artist_df = pd.DataFrame.from_dict(combined_artist_list).drop_duplicates(subset=['artist_id'])
    song_df = pd.DataFrame.from_dict(combined_song_list).drop_duplicates(subset=['song_id'])

    album_df['release_date'] = album_df['release_date'].apply(parse_release_date)
    song_df['song_added'] = song_df['song_added'].apply(parse_release_date)

    # Upload transformed files to S3
    timestamp = str(datetime.now())
    client.put_object(
        Bucket=BUCKET,
        Key=f"transformed_data/songs_data/songs_transformed_{timestamp}.csv",
        Body=song_df.to_csv(song_buffer, index=False)
    )
    client.put_object(
        Bucket=BUCKET,
        Key=f"transformed_data/album_data/album_transformed_{timestamp}.csv",
        Body=album_df.to_csv(album_buffer, index=False)
    )
    client.put_object(
        Bucket=BUCKET,
        Key=f"transformed_data/artist_data/artist_transformed_{timestamp}.csv",
        Body=artist_df.to_csv(artist_buffer, index=False)
    )

    # Move processed file
    copy_source = {'Bucket': BUCKET, 'Key': spotify_file_key}
    resource.meta.client.copy(copy_source, BUCKET, f"raw_data/processed/{spotify_file_key.split('/')[-1]}")
    resource.Object(BUCKET, spotify_file_key).delete()