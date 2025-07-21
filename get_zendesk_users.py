import os
import requests
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# === CARGA VARIABLES DE ENTORNO ===
load_dotenv()

ZENDESK_SUBDOMAIN = os.getenv('ZENDESK_SUBDOMAIN')
ZENDESK_EMAIL = os.getenv('ZENDESK_EMAIL')
ZENDESK_API_TOKEN = os.getenv('ZENDESK_API_TOKEN')

# === VALIDACIÓN DE VARIABLES ===
if not all([ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN]):
    raise EnvironmentError("⚠️ Variables de entorno faltantes en el archivo .env")

# === FECHA DE AYER EN FORMATO UTC ISO8601 ===
ayer = datetime.now(timezone.utc) - timedelta(days=1)
updated_since = ayer.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

# === ARCHIVO DE SALIDA ===
OUTPUT_FILENAME = f'usuarios_actualizados_{ayer.strftime("%Y%m%d")}.json'

# === CONFIGURACIÓN DE API ===
base_url = (
    f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/users.json'
    f'?updated_since={updated_since}&page[size]=100'
)
auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)

# === FUNCIÓN PARA OBTENER USUARIOS ACTUALIZADOS ===
def obtener_usuarios_actualizados():
    todos = []
    url_actual = base_url
    pagina = 1

    while url_actual:
        print(f'📄 Página {pagina} - {url_actual}')
        response = requests.get(url_actual, auth=auth)

        if response.status_code == 200:
            data = response.json()
            todos.extend(data.get('users', []))
            url_actual = data.get('links', {}).get('next')  # cursor-based
            pagina += 1
        else:
            print(f'❌ Error {response.status_code}: {response.text}')
            break

    return todos

# === EJECUCIÓN PRINCIPAL ===
if __name__ == '__main__':
    print(f'🚀 Consultando usuarios modificados desde: {updated_since}')
    usuarios = obtener_usuarios_actualizados()

    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(usuarios, f, indent=2, ensure_ascii=False)

    print(f'✅ Se guardaron {len(usuarios)} usuarios en: {OUTPUT_FILENAME}')

#prueba para mostrar cambios