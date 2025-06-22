# app.py
from flask import Flask, request, jsonify
import requests
import pandas as pd
from google.cloud import storage, bigquery
import io
import os
import json
app = Flask(__name__)

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'seu-projeto-gcp')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'seu-bucket-gcs')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'albion_data')

BASE_API_URL = "https://west.albion-online-data.com/api/v2/stats/prices"

@app.route('/charge-items', methods=['GET'])
def charge_items():

    nome_do_arquivo = "items.json"

    try:
        with open(nome_do_arquivo, 'r', encoding='utf-8') as arquivo:
            items = json.load(arquivo)

            languages_to_drop = [
                 'AR-SA',
                 'DE-DE',
                 'FR-FR',
                 'RU-RU',
                 'KO-KR',
                 'ZH-CN',
                 'IT-IT',
                 'JA-JP',
                 'ZH-TW',
                 'ID-ID',
                 'TR-TR',
                 'PL-PL'
            ]

            for item in items:
                item.pop('LocalizationDescriptionVariable', None)
                item.pop('LocalizationNameVariable', None)
                item.pop('Index', None)

                for language in languages_to_drop:
                    if 'LocalizedDescriptions' in item and item['LocalizedDescriptions'] is not None:
                        if language in item['LocalizedDescriptions']:
                            del item['LocalizedDescriptions'][language]
                    if 'LocalizedNames' in item and item['LocalizedNames'] is not None:
                        if language in item['LocalizedNames']:
                            del item['LocalizedNames'][language]

            df = pd.DataFrame(items)

            parquet_buffer = dataframe_to_parquet_in_memory(df)

            table_name = "dim_items"

            gcs_path = f"raw/{table_name}.parquet"

            load_to_gcs(parquet_buffer, gcs_path)

            gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
            table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

            load_to_bigquery(gcs_uri, table_id, bigquery.WriteDisposition.WRITE_TRUNCATE)

            return jsonify({
                "status": "sucesso",
                "message": f"Dimensão dos itens criada com sucesso!.",
                "tabela_bigquery": table_id
            }), 200

    except FileNotFoundError:
        return jsonify({"status": "erro", "message": "file not found"}), 422
    except json.JSONDecodeError:
        return jsonify({"status": "erro", "message": "error on decode json file"}), 422
    except Exception as e:
        return jsonify({"status": "erro", "message": str(e)}), 500

@app.route('/create-dimensions', methods=['GET'])
def create_dimensions(project_id, dataset_id, fact_table_name):
    bigquery_client = bigquery.Client(project=project_id)

    fact_table_id = f"{project_id}.{dataset_id}.{fact_table_name}"

    target_dataset = f"{project_id}.{dataset_id}"


    sql_script = f"""
    CREATE OR REPLACE TABLE `{target_dataset}.dim_city` AS
    SELECT ROW_NUMBER() OVER() AS city_key, city AS city_name
    FROM (SELECT DISTINCT city FROM {fact_table_id});
    """

    query_job = bigquery_client.query(sql_script)
    query_job.result()

def extract_data(items, locations, qualities):
    url = f"{BASE_API_URL}/{items}.json"

    query_params = {"locations": locations, "qualities": qualities}

    response = requests.get(url, params=query_params)

    response.raise_for_status()

    return response.json()


def dataframe_to_parquet_in_memory(dataFrame):
    buffer = io.BytesIO()

    dataFrame.to_parquet(buffer, index=False)

    buffer.seek(0)

    return buffer


def load_to_gcs(buffer, gcs_path):
    storage_client = storage.Client(project=GCP_PROJECT_ID)

    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    blob = bucket.blob(gcs_path)

    blob.upload_from_file(buffer, content_type='application/octet-stream')


def load_to_bigquery(gcs_uri, table_id, write_mode):
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_mode,
    )

    load_job = bigquery_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


@app.route('/fetch-data', methods=['GET'])
def trigger_pipeline():
    try:
        items = request.args.get("items", "T4_BAG")
        locations = request.args.get("locations", "Caerleon")
        qualities = request.args.get("qualities", "1,2")

        write_mode_str = request.args.get("mode", "APPEND").upper()

        write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        if write_mode_str == "TRUNCATE":
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        table_name = "item_prices"

        api_data = extract_data(items, locations, qualities)

        df = pd.DataFrame(api_data)

        parquet_buffer = dataframe_to_parquet_in_memory(df)

        gcs_path = f"raw/{table_name}.parquet"

        load_to_gcs(parquet_buffer, gcs_path)

        gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

        load_to_bigquery(gcs_uri, table_id, write_disposition)

        create_dimensions(GCP_PROJECT_ID, BIGQUERY_DATASET, table_name)

        return jsonify({
            "status": "sucesso",
            "message": f"Dados para '{items}' foram processados.",
            "tabela_bigquery": table_id
        }), 200

    except Exception as e:
        return jsonify({"status": "erro", "message": str(e)}), 500


# Bloco para rodar o servidor Flask localmente para testes
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))