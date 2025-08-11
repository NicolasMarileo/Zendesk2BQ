# main_general.py

from extract.extract_org import extract_organizations_incremental
from load.load_org import load_organizations_incremental

from extract.extract_extra_fields import extract_and_flatten_modified_organizations
from load.load_extra_fields import load_if_changed

from extract.extract_memberships import extract_organization_memberships
from load.load_membership import load_incremental_to_bigquery

import os
from dotenv import load_dotenv

try:
    load_dotenv()
except Exception:
    pass  # No es necesario en entorno cloud

def main():
    print("🚀 Iniciando pipeline general Zendesk → BigQuery")
    
    load_dotenv()

    #### --- 1. ORGANIZACIONES --- ####
    print("\n📂 [1/3] Cargando Organizaciones...")
    try:
        orgs = extract_organizations_incremental()
        load_organizations_incremental(orgs)
    except Exception as e:
        print(f"❌ Error en carga de organizaciones: {e}")

    #### --- 2. EXTRA FIELDS --- ####
    print("\n📂 [2/3] Cargando Extra Fields de Organizaciones...")
    try:
        subdomain = os.getenv("ZENDESK_SUBDOMAIN")
        email = os.getenv("ZENDESK_EMAIL")
        token = os.getenv("ZENDESK_TOKEN")
        df_extras = extract_and_flatten_modified_organizations(subdomain, email, token)
        load_if_changed(df_extras, "testbigquerydimarsa", "Zendesk.Organizaciones_ExtraFields")
    except Exception as e:
        print(f"❌ Error en carga de extra fields: {e}")

    #### --- 3. ORGANIZATION MEMBERSHIPS --- ####
    print("\n📂 [3/3] Cargando Organization Memberships...")
    try:
        memberships = extract_organization_memberships()
        load_incremental_to_bigquery(memberships)
    except Exception as e:
        print(f"❌ Error en carga de memberships: {e}")

    print("\n✅ Pipeline completado.")

if __name__ == "__main__":
    main()