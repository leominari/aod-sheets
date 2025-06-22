# app.py
from flask import Flask, request, jsonify
import requests
import pandas as pd
from google.cloud import storage, bigquery
import io
import os  # Para ler variáveis de ambiente

# --- Inicialização do Flask ---
app = Flask(__name__)

# --- Configurações lidas de Variáveis de Ambiente ---
# É uma boa prática não deixar "segredos" ou configurações no código.
# Vamos configurar isso no Cloud Run depois.
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'seu-projeto-gcp')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'seu-bucket-gcs')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'albion_data')
BASE_API_URL = "https://west.albion-online-data.com/api/v2/stats/prices"


# As funções auxiliares do pipeline continuam as mesmas
def extract_data(items, locations, qualities):
    # ... (mesma função da resposta anterior)
    print(f"Extraindo dados para os itens: {items}")
    url = f"{BASE_API_URL}/{items}.json"
    query_params = {"locations": locations, "qualities": qualities}
    response = requests.get(url, params=query_params)
    response.raise_for_status()

    return response.json()


def transform_to_parquet_in_memory(data):
    # ... (mesma função da resposta anterior)
    print("Transformando dados...")
    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def load_to_gcs(buffer, gcs_path):
    # ... (mesma função da resposta anterior)
    print(f"Fazendo upload para GCS: {gcs_path}")
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type='application/octet-stream')


def load_to_bigquery(gcs_uri, table_id, write_mode):
    # ... (função modificada para aceitar o modo de escrita)
    print(f"Carregando dados na tabela '{table_id}' do BigQuery...")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_mode,  # Usando o modo dinâmico
    )
    load_job = bigquery_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


# --- Endpoint da API Flask ---
@app.route('/run-pipeline', methods=['GET'])
def trigger_pipeline():
    """
    Este é o nosso endpoint. Ele será acionado quando alguém acessar
    SUA_URL/run-pipeline.
    """
    try:
        # Lendo os parâmetros da URL
        items = request.args.get("items", "T5_BAG")
        locations = request.args.get("locations", "Caerleon,Bridgewatch")
        qualities = request.args.get("qualities", "1,2,3")

        # Novo parâmetro para controlar se apaga ou adiciona dados
        write_mode_str = request.args.get("write_mode", "APPEND").upper()
        if write_mode_str == "TRUNCATE":
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Orquestração do Pipeline
        table_name = items.split(',')[0].replace("_", "-").lower()  # Ex: t5-bag

        api_data = extract_data(items, locations, qualities)
        parquet_buffer = transform_to_parquet_in_memory(api_data)

        gcs_path = f"raw/{table_name}.parquet"
        load_to_gcs(parquet_buffer, gcs_path)

        gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        load_to_bigquery(gcs_uri, table_id, write_disposition)

        # Retornamos uma resposta JSON
        return jsonify({
            "status": "sucesso",
            "message": f"Dados para '{items}' foram processados.",
            "tabela_bigquery": table_id
        }), 200

    except Exception as e:
        print(f"❌ Ocorreu um erro no pipeline: {e}")
        return jsonify({"status": "erro", "message": str(e)}), 500


# Bloco para rodar o servidor Flask localmente para testes
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))