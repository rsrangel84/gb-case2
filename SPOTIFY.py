import os
import base64
import requests
import json
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "application_default_credentials.json"
os.environ["GCLOUD_PROJECT"] = ""
client_id = ''
client_secret = ''

def get_spotify_access_token(client_id, client_secret):
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + base64.b64encode((client_id + ':' + client_secret).encode('ascii')).decode('ascii')
    }
    
    data = {
        'grant_type': 'client_credentials'
    }
    
    response = requests.post(url, headers=headers, data=data)
    data = response.json()   
    access_token = data['access_token']
    return access_token

url_busca = 'https://api.spotify.com/v1/search'
url_episodios = 'https://api.spotify.com/v1/shows/{id}/episodes'

params_busca = {
    'q': 'data+hackers',
    'type': 'show',
    'limit' : 50,
    'offset' : 0,
    'market' : 'BR'
}

access_token = get_spotify_access_token(client_id, client_secret)

headers = {
    'Authorization': 'Bearer ' + access_token
}

response = requests.get(url_busca, params=params_busca, headers=headers)
dados_busca = json.loads(response.text)

podcasts = []
episodios = []
episodios_gb = []

for podcast in dados_busca['shows']['items']:
  podcast_id = podcast['id']
  podcast_name = podcast['name']
  podcast_description = podcast['description']
  podcast_total_episodes = podcast['total_episodes']
  podcasts.append((podcast_id, podcast_name, podcast_description, podcast_total_episodes))
 
  if podcast_name == 'Data Hackers':
    proxima_url = 'https://api.spotify.com/v1/shows/'+podcast['id']+'/episodes?offset=0&limit=50&market=BR'  
    
    while proxima_url is not None:
      response = requests.get(proxima_url, headers=headers)    
      dados_episodio = json.loads(response.text)
      proxima_url = dados_episodio['next']

      for episodio in dados_episodio['items']:
        episodio_id = (episodio['id'])
        episodio_name = (episodio['name'])
        episodio_description = (episodio['description'])
        episodio_release_date = (episodio['release_date'])
        episodio_duration_ms = (episodio['duration_ms'])
        episodio_language = (episodio['language'])   
        episodio_explicit = (episodio['explicit'])     
        episodio_type = (episodio['type'])               
        episodios.append((episodio_id, episodio_name, episodio_description, episodio_release_date, episodio_duration_ms, episodio_language, episodio_explicit, episodio_type))
        
        if episodio['description'].find("Grupo Boticário") > 0:
          episodios_gb.append((episodio_id, episodio_name, episodio_description, episodio_release_date, episodio_duration_ms, episodio_language, episodio_explicit, episodio_type))

client = bigquery.Client()
dataset_id = 'SPOTIFY'
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset = client.create_dataset(dataset, exists_ok=True)

#Tabela Podcast
podcasts_schema = [
    bigquery.SchemaField('podcast_id', 'STRING'),
    bigquery.SchemaField('podcast_name', 'STRING'),
    bigquery.SchemaField('podcast_description', 'STRING'),
    bigquery.SchemaField('podcast_total_episodes', 'INTEGER'),
]

table_podcasts_ref = dataset.table('TAB5_podcasts')
table_podcasts = bigquery.Table(table_podcasts_ref, schema=podcasts_schema)
table_podcasts = client.create_table(table_podcasts, exists_ok=True)

if podcasts:
    errors = client.insert_rows(table_podcasts, podcasts)
    if not errors:
        print('Tabela Podcast atualizada com sucesso')
    else:
        print('Erros:', errors)
else:
    print('Sem dados para inserir')

#Tabela Episodios
episodios_schema = [
    bigquery.SchemaField('episodio_id', 'STRING'),
    bigquery.SchemaField('episodio_name', 'STRING'),
    bigquery.SchemaField('episodio_description', 'STRING'),
    bigquery.SchemaField('episodio_release_date', 'DATETIME'),
    bigquery.SchemaField('episodio_duration_ms', 'INTEGER'),
    bigquery.SchemaField('episodio_language', 'STRING'),
    bigquery.SchemaField('episodio_explicit', 'BOOLEAN'),
    bigquery.SchemaField('episodio_type', 'STRING'), 
]

table_episodios_ref = dataset.table('TAB6_episodios')
table_episodios = bigquery.Table(table_episodios_ref, schema=episodios_schema)
table_episodios = client.create_table(table_episodios, exists_ok=True)

if episodios:
    errors = client.insert_rows(table_episodios, episodios)
    if not errors:
        print('Tabela Episodios atualizada com sucesso')
    else:
        print('Erros:', errors)
else:
    print('Sem dados para inserir')


#Tabela Episodios GB
table_episodios_gb_ref = dataset.table('TAB7_episodios_gb')
table_episodios_gb = bigquery.Table(table_episodios_gb_ref, schema=episodios_schema)
table_episodios_gb = client.create_table(table_episodios_gb, exists_ok=True)

if episodios_gb:
    errors = client.insert_rows(table_episodios_gb, episodios_gb)
    if not errors:
        print('Tabela Episodios GB atualizada com sucesso')
    else:
        print('Erros:', errors)
else:
    print('Sem dados para inserir')
