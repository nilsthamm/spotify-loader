import json
from google.cloud import bigquery
from google.oauth2 import service_account
import tekore as tk

import os
spotify_access_token = os.environ['spotify_access_token']
gcp_project_id = os.environ['gcp_project_id']

spotify = tk.Spotify(spotify_access_token)

credentials = service_account.Credentials.from_service_account_file(
    "/secrets/sa_key", 
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

job_config = bigquery.LoadJobConfig(
    autodetect=True, 
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

uid = spotify.current_user().id
results = spotify.playlists(uid, limit=50, offset=0)
playlists = results.items
while results.next:
    results = spotify.next(results)
    playlists.extend(results.items)

playlists = [pl for pl in playlists if "Your Top Songs" in pl.name]
json_payload = []

for pl in playlists:
    tracks = []
    print(pl.name)
    results = spotify.playlist_items(pl.id, limit=100, offset=0)
    tracks.extend(results.items)
    while  results.next:
        results = spotify.next(results)
        tracks.extend(results.items)
    
    json_payload.extend ([{
        'playlist_name': pl.name, \
        'track_id': t.track.id, \
        'track_name': t.track.name, \
        'added_at': str(t.added_at), \
        'artist_name': t.track.artists[0].name, \
        'artist_id': t.track.artists[0].id, \
        'duration_ms': t.track.duration_ms, \
        'album_release_date': str(t.track.album.release_date), 
        'explicit': t.track.explicit, 
        'popularity': t.track.popularity, 
        'preview_url': t.track.preview_url
        }  for t in tracks])

job = client.load_table_from_json(destination=f'{gcp_project_id}.prod.raw_playlist_data', json_rows=json_payload, job_config=job_config)
