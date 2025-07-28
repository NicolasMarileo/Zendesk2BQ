import os
import requests
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google.auth.exceptions import DefaultCredentialsError
from io import BytesIO

# === CARGA VARIABLES DE ENTORNO ===
load_dotenv()

ZENDESK_SUBDOMAIN = os.getenv('ZENDESK_SUBDOMAIN')
ZENDESK_EMAIL = os.getenv('ZENDESK_EMAIL')
ZENDESK_API_TOKEN = os.getenv('ZENDESK_API_TOKEN')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')

if not all([ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN, GCP_PROJECT_ID, BUCKET_NAME]):
    raise EnvironmentError("⚠️ Faltan variables de entorno. Verifica tu archivo .env")

# === CONFIGURACIÓN GENERAL ===
fecha_hoy = datetime.now(timezone.utc).strftime("%Y%m%d")
storage_filename = f'usuarios_completos_{fecha_hoy}.json'
bq_table = 'testbigquerydimarsa.Zendesk.Usuarios'
base_url = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/users.json?page[size]=100'
auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)

# === CONSULTA CON CURSOR PAGINATION ===
def obtener_todos_los_usuarios():
    todos = []
    url_actual = base_url  # e.g. ".../users.json?page[size]=100"
    pagina = 1

    while url_actual:
        print(f'📄 Página {pagina} - {url_actual}')
        response = requests.get(url_actual, auth=auth)

        if response.status_code == 200:
            data = response.json()
            todos.extend(data.get('users', []))

            # Paginación con cursor
            if data.get('meta', {}).get('has_more', False):
                url_actual = data.get('links', {}).get('next')
            else:
                url_actual = None

            pagina += 1
            time.sleep(0.5)

        elif response.status_code == 429:
            retry = int(response.headers.get('Retry-After', 60))
            print(f'⏳ Rate limit. Esperando {retry}s...')
            time.sleep(retry)

        else:
            print(f'❌ Error {response.status_code}: {response.text}')
            break

    return todos

# === SUBE NDJSON A CLOUD STORAGE ===
def subir_a_storage(bucket_name: str, nombre_archivo: str, lista_json: list, project_id: str):
    print(f'☁️ Subiendo respaldo NDJSON a GCS: {bucket_name}/{nombre_archivo}')
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(nombre_archivo)

        ndjson = '\n'.join(json.dumps(obj, ensure_ascii=False) for obj in lista_json)
        blob.upload_from_string(
            data=ndjson,
            content_type='application/x-ndjson'
        )
        print('✅ Archivo NDJSON subido exitosamente.')
    except DefaultCredentialsError:
        print("❌ Error de credenciales. Ejecuta 'gcloud auth application-default login'.")
        raise

# === CARGA A BIGQUERY DESDE NDJSON EN MEMORIA ===
def cargar_a_bigquery_desde_ndjson(lista_json: list, table_id: str, project_id: str):
    print(f'📥 Cargando datos directamente a BigQuery → {table_id}')
    client = bigquery.Client(project=project_id)

    ndjson_str = '\n'.join(json.dumps(obj, ensure_ascii=False) for obj in lista_json)
    file_like = BytesIO(ndjson_str.encode('utf-8'))

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True  # 🔐 IGNORA CAMPOS EXTRA
    )

    load_job = client.load_table_from_file(
        file_obj=file_like,
        destination=table_id,
        job_config=job_config
    )
    load_job.result()
    print('✅ Carga completada.')

    table = client.get_table(table_id)
    print(f'📊 Filas totales en la tabla: {table.num_rows}')

# === MAIN ===
def main():
    print('🚀 Consultando TODOS los usuarios de Zendesk...')
    usuarios = obtener_todos_los_usuarios()
    print(f'📦 Total de usuarios obtenidos: {len(usuarios)}')

    if usuarios:
        subir_a_storage(BUCKET_NAME, storage_filename, usuarios, GCP_PROJECT_ID)
        cargar_a_bigquery_desde_ndjson(usuarios, bq_table, GCP_PROJECT_ID)

if __name__ == '__main__':
    main()