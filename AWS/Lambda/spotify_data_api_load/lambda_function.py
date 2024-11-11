import json
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO

#Low level API. Provides more control
client = boto3.client('s3')
Bucket = 'spotify-snowflake-etl-pavan'
Key = 'raw_data/to_process/'
song_buffer = StringIO()
album_buffer = StringIO()
artist_buffer = StringIO()
#High level API
resource = boto3.resource('s3')

def get_album_list(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id, 'name':album_name, 'release_date':album_release_date,
                         'total_tracks':album_total_tracks, 'url':album_url}
        album_list.append(album_element)
    return album_list

def get_artist_list(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'],
                                  'external_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list

def get_song_list(data):
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
        song_element = {'song_id':song_id, 'song_name':song_name, 'duration_ms': song_duration,
                       'url': song_url, 'popularity': song_popularity, 'song_added':song_added,
                       'album_id':album_id, 'artist_id':artist_id}
        song_list.append(song_element)
    return song_list

def parse_release_date(date):
    try:
        # Check if the date is just a year (4 characters long)
        if len(date) == 4:
            # Append "-01-01" to create a valid date for the start of the year
            return pd.to_datetime(date + "-01-01")
        else:
            # Otherwise, parse the date as it is
            return pd.to_datetime(date)
    except:
        return pd.NaT

def lambda_handler(event, context):
    spotify_data = []
    spotify_keys = []
    for file in client.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key=file['Key']
        if file_key.split('.')[-1] == 'json':
            response = client.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObj = json.loads(content.read())
            spotify_data.append(jsonObj)
            spotify_keys.append(file_key)
    print("Spotify data", spotify_data)
    print("Spotify key", spotify_keys)

    print("Hello")
    i = 0

    #This logic is to transform the data and store it in the transformed_data folder
    for data in spotify_data:
        print("i", i)
        i = i + 1
        print("Data in for loop", data)
        album_list = get_album_list(data)
        artist_list = get_artist_list(data)
        song_list = get_song_list(data)

        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])

        song_df = pd.DataFrame.from_dict(song_list)
        song_df = song_df.drop_duplicates(subset=['song_id'])

        album_df['release_date'] = album_df['release_date'].apply(parse_release_date)
        song_df['song_added'] = song_df['song_added'].apply(parse_release_date)

        song_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    #This logic is to move the data from to_process to processed folder
    #So that we do not process the same files repeatedly
    #There is no move in boto3 so we first copy the data to processed folder and then delete from to_process
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        resource.Object(Bucket, key).delete()
