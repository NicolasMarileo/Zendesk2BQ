import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "testbigquerydimarsa"
DATASET_ID = "Zendesk"
TABLE_NAME = "OrganizationMemberships"

def load_incremental_to_bigquery(memberships):
    df_new = pd.DataFrame(memberships)

    # Normalizar fechas
    df_new["created_at"] = pd.to_datetime(df_new["created_at"])
    df_new["updated_at"] = pd.to_datetime(df_new["updated_at"])

    expected_cols = [
        "id", "user_id", "organization_id", "default",
        "created_at", "updated_at", "organization_name",
        "view_tickets", "url"
    ]
    df_new = df_new[expected_cols]

    client = bigquery.Client()

    # Leer tabla actual desde BigQuery
    query = f"""
        SELECT id, updated_at
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}`
    """
    df_existing = client.query(query).to_dataframe()

    # Detectar nuevos o modificados
    merged = pd.merge(
        df_new, df_existing,
        on="id",
        how="left",
        suffixes=("", "_existing")
    )

    df_delta = merged[
        merged["updated_at_existing"].isna() |
        (merged["updated_at"] != merged["updated_at_existing"])
    ][expected_cols]

    # Detectar eliminados (ids que existen en BQ pero no en la API)
    ids_in_api = set(df_new["id"])
    ids_in_bq = set(df_existing["id"])
    ids_to_delete = [int(x) for x in (ids_in_bq - ids_in_api)]  # ✅ conversión a int

    # Insertar o actualizar registros nuevos/modificados
    if not df_delta.empty:
        staging_table = f"{DATASET_ID}._stg_OrganizationMemberships"
        staging_table_id = f"{PROJECT_ID}.{staging_table}"

        client.load_table_from_dataframe(df_delta, staging_table_id).result()

        merge_sql = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}` T
        USING `{staging_table_id}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET
            user_id = S.user_id,
            organization_id = S.organization_id,
            `default` = S.`default`,
            created_at = S.created_at,
            updated_at = S.updated_at,
            organization_name = S.organization_name,
            view_tickets = S.view_tickets,
            url = S.url
        WHEN NOT MATCHED THEN
          INSERT (
            id, user_id, organization_id, `default`, created_at,
            updated_at, organization_name, view_tickets, url
          )
          VALUES (
            S.id, S.user_id, S.organization_id, S.`default`, S.created_at,
            S.updated_at, S.organization_name, S.view_tickets, S.url
          )
        """
        client.query(merge_sql).result()
        client.delete_table(staging_table_id, not_found_ok=True)
        print(f"✔ {len(df_delta)} registros nuevos/modificados procesados.")

    # Eliminar registros que ya no existen en Zendesk
    if ids_to_delete:
        delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}`
        WHERE id IN UNNEST(@ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "INT64", ids_to_delete)
            ]
        )
        client.query(delete_query, job_config=job_config).result()
        print(f"🗑️ {len(ids_to_delete)} registros eliminados.")

    # Mensaje final si no hubo cambios
    if df_delta.empty and not ids_to_delete:
        print("✅ No hay registros nuevos, modificados ni eliminados.")


