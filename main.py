import os
from pysus.online_data import SINAN
import pandas as pd
from google.cloud import storage
from datetime import datetime

def run_oda_pipeline_tube():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/tuberculose"  # Pasta atualizada para tuberculose
    COD_ALAGOINHAS = "290070"
    
    print("Conectando ao SINAN...")
    sinan = SINAN.SINAN().load()
    
    # 2. Busca apenas 2026
    print("Buscando dados de Tuberculose de 2026...")
    arquivos = sinan.get_files(dis_code="TUBE", year=2026)  # Código do agravo alterado para TUBE
    
    if not arquivos:
        print("Nenhum arquivo de 2026 encontrado no servidor para Tuberculose.")
        return

    # 3. Download e Filtro
    print(f"Baixando {arquivos[0].name}...")
    arquivo_baixado = arquivos[0].download()
    df = arquivo_baixado.to_dataframe()
    
    df_alagoinhas = df[df['ID_MN_RESI'] == COD_ALAGOINHAS]
    
    if df_alagoinhas.empty:
        print("Nenhum dado novo de Tuberculose para Alagoinhas nesta carga.")
        return

    # 4. Preparação do arquivo para o Storage
    local_filename = "tuberculose_alagoinhas_2026.csv"  # Nome do arquivo ajustado
    df_alagoinhas.to_csv(local_filename, index=False, sep=';', encoding='utf-8')
    
    # 5. Upload para o Cloud Storage
    print(f"Subindo {local_filename} para o bucket {BUCKET_NAME}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{DESTINATION_FOLDER}/{local_filename}")
    
    blob.upload_from_filename(local_filename)
    print(f"Sucesso! Arquivo disponível em {DESTINATION_FOLDER}/{local_filename}")

if __name__ == "__main__":
    run_oda_pipeline_tube()