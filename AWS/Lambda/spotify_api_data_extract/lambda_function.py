client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
client = boto3.client('s3')
    
def lambda_handler(event, context):
    playlist_links = ["https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M", "https://open.spotify.com/playlist/37i9dQZF1DX0ieekvzt1Ic", 
        "https://open.spotify.com/playlist/37i9dQZF1DWY4lFlS4Pnso", "https://open.spotify.com/playlist/37i9dQZF1DX0kbJZpiYdZl"]
    playlist_names = ["TopHits", "HotHitsIndia", "HotHitsUK", "HotHitsUSA"]

    #Dictionary where the key is a playlist name and the value is the playlist_data
    spotify_data = {}

    for i in range(len(playlist_links)):
        playlist_URI = playlist_links[i].split('/')[-1]
        playlist_data = sp.playlist_tracks(playlist_URI)
        spotify_data[playlist_names[i]] = playlist_data
   
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    #This is the Extract part -> Lambda pushes the unstructured data to S3
    client.put_object(
        Bucket="spotify-snowflake-etl-pavan",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(spotify_data)
    )