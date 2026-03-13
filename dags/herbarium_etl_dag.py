from datetime import datetime
import json
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


SOURCE_TABLE = "herbarium_tasks"
TARGET_TABLE = "ci_herbarium_specimens"


default_args = {
    "owner": "airflow",
}


# -------------------------------
# Barcode Normalization Function
# -------------------------------
def normalize_barcode(barcode):

    if not barcode or not barcode.startswith("LWG"):
        return None

    # extract digits after LWG
    digits = re.sub(r"\D", "", barcode[3:])

    # keep only last 9 digits
    digits = digits[-9:]

    # fill remaining with zeros from left
    digits = digits.zfill(9)

    return f"LWG{digits}"


# -------------------------------
# ETL PROCESS
# -------------------------------
def etl_process():

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    # ---------- EXTRACT ----------
    cur.execute(f"SELECT * FROM {SOURCE_TABLE}")
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    for row in rows:

        record = dict(zip(columns, row))

        taxonomy_data = record.get("taxonomy_data")
        barcode = record.get("barcode")

        # ---------- TRANSFORM ----------

        # TEXT → JSON
        try:
            taxonomy_json = json.loads(taxonomy_data) if taxonomy_data else None
        except:
            taxonomy_json = None

        # Barcode normalization
        barcode = normalize_barcode(barcode)

        genus = record.get("genus")
        species = record.get("species")

        specimen_name = None
        if genus or species:
            specimen_name = f"{genus or ''} {species or ''}".strip()

        # ---------- LOAD ----------

        insert_query = f"""
        INSERT INTO {TARGET_TABLE} (
            id,
            barcode,
            accession_number,
            collection_no,
            collection_date,
            specimen_name,
            vernacular_name,
            family_name,
            genius_name,
            species_name,
            taxonomy_data,
            collector_name,
            locality_name,
            collection_latitude,
            collection_longitude,
            collection_altitude,
            original_image,
            specimen_details,
            specimen_status
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (id) DO NOTHING
        """

        values = (
            record.get("id"),
            barcode,
            record.get("accession_number"),
            record.get("collection_number"),
            record.get("collection_date"),
            specimen_name,
            record.get("common_name"),
            record.get("family"),
            record.get("genus"),
            record.get("species"),
            json.dumps(taxonomy_json) if taxonomy_json else None,
            record.get("collector_name"),
            record.get("location"),
            record.get("latitude"),
            record.get("longitude"),
            record.get("altitude"),
            record.get("image_url"),
            record.get("notes"),
            record.get("status"),
        )

        cur.execute(insert_query, values)

    conn.commit()

    cur.close()
    conn.close()


# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="herbarium_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=etl_process,
    )

    run_etl