# from google.cloud import bigquery

# def load_organizations_incremental(rows):
#     BQ_PROJECT = "testbigquerydimarsa"
#     BQ_DATASET = "Zendesk"
#     BQ_TABLE = "Organizaciones"

#     client = bigquery.Client(project=BQ_PROJECT)
#     table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
#     temp_table_id = f"{BQ_PROJECT}.{BQ_DATASET}._temp_Organizaciones"

#     # Transformar los datos con validación robusta de tipos
#     def transform(row):
#         def parse_int(value):
#             try:
#                 return int(value)
#             except (TypeError, ValueError):
#                 return None

#         return {
#             'id': parse_int(row.get('id')),
#             'name': row.get('name'),
#             'domain_names': row.get('domain_names') or [],
#             'created_at': row.get('created_at'),
#             'updated_at': row.get('updated_at'),
#             'details': row.get('details'),
#             'notes': row.get('notes'),
#             'group_id': parse_int(row.get('group_id')),
#             'shared_tickets': row.get('shared_tickets'),
#             'shared_comments': row.get('shared_comments'),
#             'tags': row.get('tags') or [],
#             'external_id': row.get('external_id'),
#             'url': row.get('url')
#         }

#     rows_to_insert = [transform(row) for row in rows]

#     # Carga a tabla temporal con autodetección
#     job_config = bigquery.LoadJobConfig(
#         write_disposition="WRITE_TRUNCATE",
#         autodetect=True
#     )

#     load_job = client.load_table_from_json(
#         rows_to_insert, temp_table_id, job_config=job_config
#     )
#     load_job.result()

#     # MERGE con CASTs seguros
#     merge_query = f"""
#         MERGE `{table_id}` T
#         USING (
#             SELECT
#                 CAST(id AS INT64) AS id,
#                 name,
#                 domain_names,
#                 TIMESTAMP(created_at) as created_at,
#                 TIMESTAMP(updated_at) as updated_at,
#                 details,
#                 notes,
#                 CAST(group_id AS INT64) AS group_id,
#                 shared_tickets,
#                 shared_comments,
#                 tags,
#                 external_id,
#                 url
#             FROM `{temp_table_id}`
#         ) S
#         ON T.id = S.id
#         WHEN MATCHED THEN UPDATE SET
#             name = S.name,
#             domain_names = S.domain_names,
#             created_at = S.created_at,
#             updated_at = S.updated_at,
#             details = S.details,
#             notes = S.notes,
#             group_id = S.group_id,
#             shared_tickets = S.shared_tickets,
#             shared_comments = S.shared_comments,
#             tags = S.tags,
#             external_id = S.external_id,
#             url = S.url
#         WHEN NOT MATCHED THEN INSERT ROW;
#     """

#     client.query(merge_query).result()
#     client.delete_table(temp_table_id, not_found_ok=True)

#     print(f"MERGE incremental completado con {len(rows_to_insert)} registros.")

from google.cloud import bigquery

def load_organizations_incremental(rows):
    BQ_PROJECT = "testbigquerydimarsa"
    BQ_DATASET = "Zendesk"
    BQ_TABLE = "Organizaciones"

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    temp_table_id = f"{BQ_PROJECT}.{BQ_DATASET}._temp_Organizaciones"

    # Transformar los datos con validación robusta de tipos
    def transform(row):
        def parse_int(value):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        return {
            'id': parse_int(row.get('id')),
            'name': row.get('name'),
            'domain_names': row.get('domain_names') or [],
            'created_at': row.get('created_at'),
            'updated_at': row.get('updated_at'),
            'details': row.get('details'),
            'notes': row.get('notes'),
            'group_id': parse_int(row.get('group_id')),
            'shared_tickets': row.get('shared_tickets'),
            'shared_comments': row.get('shared_comments'),
            'tags': row.get('tags') or [],
            'external_id': row.get('external_id'),
            'url': row.get('url')
        }

    rows_to_insert = [transform(row) for row in rows]

    if not rows_to_insert:
        print("⚠ No hay registros para cargar.")
        return

    # Carga a tabla temporal con autodetección
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )

    load_job = client.load_table_from_json(
        rows_to_insert, temp_table_id, job_config=job_config
    )
    load_job.result()

    # MERGE con CASTs seguros
    merge_query = f"""
        MERGE `{table_id}` T
        USING (
            SELECT
                CAST(id AS INT64) AS id,
                name,
                domain_names,
                TIMESTAMP(created_at) as created_at,
                TIMESTAMP(updated_at) as updated_at,
                details,
                notes,
                CAST(group_id AS INT64) AS group_id,
                shared_tickets,
                shared_comments,
                tags,
                external_id,
                url
            FROM `{temp_table_id}`
        ) S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
            name = S.name,
            domain_names = S.domain_names,
            created_at = S.created_at,
            updated_at = S.updated_at,
            details = S.details,
            notes = S.notes,
            group_id = S.group_id,
            shared_tickets = S.shared_tickets,
            shared_comments = S.shared_comments,
            tags = S.tags,
            external_id = S.external_id,
            url = S.url
        WHEN NOT MATCHED THEN INSERT ROW;
    """

    client.query(merge_query).result()
    client.delete_table(temp_table_id, not_found_ok=True)

    print(f"✔ MERGE incremental completado con {len(rows_to_insert)} registros insertados/actualizados.")

