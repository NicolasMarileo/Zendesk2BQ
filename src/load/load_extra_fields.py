from google.cloud import bigquery
import pandas as pd

def load_if_changed(df_new: pd.DataFrame, project_id: str, table_ref: str):
    """
    Compara df_new con datos actuales en BigQuery. Si hay diferencias, actualiza solo los registros modificados.
    """
    client = bigquery.Client(project=project_id)

    print("📥 Obteniendo tabla actual de BigQuery...")
    df_current = client.query(f"SELECT * FROM `{table_ref}`").to_dataframe()

    if df_current.empty:
        print("⚠️ Tabla actual vacía, se hará carga completa.")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("id", "INT64"),
                bigquery.SchemaField("nombre_atributo", "STRING"),
                bigquery.SchemaField("valor_atributo", "STRING"),
            ]
        )
        load_job = client.load_table_from_dataframe(df_new, table_ref, job_config=job_config)
        load_job.result()
        print(f"✅ Carga inicial completada: {load_job.output_rows} filas.")
        return

    # Quitar filas antiguas de los IDs modificados
    ids_modificados = df_new["id"].unique().tolist()
    df_filtrado = df_current[~df_current["id"].isin(ids_modificados)]

    # Concatenar antiguos + nuevos
    df_final = pd.concat([df_filtrado, df_new], ignore_index=True)

    # Subir resultado final
    print(f"📤 Cargando {len(df_final)} filas finales a BigQuery...")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("nombre_atributo", "STRING"),
            bigquery.SchemaField("valor_atributo", "STRING"),
        ]
    )
    load_job = client.load_table_from_dataframe(df_final, table_ref, job_config=job_config)
    load_job.result()
    print(f"✅ Tabla actualizada. Filas cargadas: {load_job.output_rows}")

