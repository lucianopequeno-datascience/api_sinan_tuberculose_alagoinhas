import os
import sys
from pysus.online_data import SINAN
import pandas as pd
from google.cloud import storage

def run_oda_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/tuberculose"
    COD_ALAGOINHAS = "290070"
    
    # Intervalo da série histórica
    ANOS = range(2020, 2027) 
    
    print("Iniciando pipeline de Tuberculose...")
    try:
        sinan = SINAN.SINAN().load()
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"ERRO CRÍTICO NA INICIALIZAÇÃO: {e}")
        sys.exit(1) # Só encerra se falhar a conexão com a nuvem
    
    for year in ANOS:
        print(f"--- Processando ano: {year} ---")
        
        try:
            # Busca os arquivos
            arquivos = sinan.get_files(dis_code="TUBE", year=year)
            
            if not arquivos:
                print(f"INFO: Nenhum dado disponível no servidor para o ano {year}. Pulando...")
                continue

            # Download e processamento
            print(f"Baixando dados de {year}...")
            df = arquivos[0].download().to_dataframe()
            
            # Filtro
            if 'ID_MN_RESI' in df.columns:
                df_alagoinhas = df[df['ID_MN_RESI'] == COD_ALAGOINHAS]
            else:
                print(f"AVISO: Coluna 'ID_MN_RESI' não encontrada em {year}. Pulando.")
                continue
            
            if df_alagoinhas.empty:
                print(f"INFO: Nenhum registro para Alagoinhas em {year}.")
                continue
                
            # Upload
            local_filename = f"tuberculose_alagoinhas_{year}.csv"
            df_alagoinhas.to_csv(local_filename, index=False, sep=';', encoding='utf-8')
            
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{local_filename}")
            blob.upload_from_filename(local_filename)
            print(f"SUCESSO: Arquivo {local_filename} enviado.")
            
            # Limpeza local
            if os.path.exists(local_filename):
                os.remove(local_filename)
                
        except Exception as e:
            # Captura erros inesperados de um ano específico sem derrubar a execução dos outros
            print(f"AVISO: Não foi possível processar o ano {year}. Detalhe: {e}")
            continue

    print("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    run_oda_pipeline()
