import requests
import pandas as pd
from datetime import datetime, timedelta

def extract_and_flatten_modified_organizations(subdomain, email, token):
    """
    Extrae organizaciones modificadas ayer y aplana sus campos personalizados.
    """
    # Obtener timestamp del inicio de ayer
    ayer = datetime.utcnow() - timedelta(days=1)
    start_time = int(ayer.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    url = f"https://{subdomain}.zendesk.com/api/v2/incremental/organizations.json?start_time={start_time}"
    auth = (f"{email}/token", token)

    all_orgs = []
    seen_ids = set()

    while url:
        print(f"📡 Solicitando: {url}")
        resp = requests.get(url, auth=auth)
        resp.raise_for_status()
        data = resp.json()

        for org in data.get("organizations", []):
            if org["id"] not in seen_ids:
                seen_ids.add(org["id"])
                all_orgs.append(org)

        url = data.get("next_page") if data.get("end_of_stream") is False else None

    print(f"✅ Organizaciones modificadas ayer: {len(all_orgs)}")

    # Aplanar organization_fields
    flat = []
    for org in all_orgs:
        org_id = org.get("id")
        fields = org.get("organization_fields", {})
        for key, value in fields.items():
            flat.append({
                "id": org_id,
                "nombre_atributo": key,
                "valor_atributo": str(value)
            })

    return pd.DataFrame(flat)




