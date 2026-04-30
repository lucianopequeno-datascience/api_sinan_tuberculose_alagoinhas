import os
from pysus.online_data import SINAN
import pandas as pd
from google.cloud import storage

def run_oda_pipeline_tube_history():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/tuberculose"
    COD_ALAGOINHAS = "290070"
    
    # Intervalo da série histórica (ex: 2020 a 2026)
    ANOS = range(2020, 2027) 
    
    print("Conectando ao SINAN...")
    sinan = SINAN.SINAN().load()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    for year in ANOS:
        print(f"\n--- Processando ano: {year} ---")
        
        # 2. Busca o arquivo do ano
        arquivos = sinan.get_files(dis_code="TUBE", year=year)
        
        if not arquivos:
            print(f"Nenhum arquivo encontrado para o ano {year}. Pulando...")
            continue

        # 3. Download e Filtro
        print(f"Baixando {arquivos[0].name}...")
        try:
            df = arquivos[0].download().to_dataframe()
            df_alagoinhas = df[df['ID_MN_RESI'] == COD_ALAGOINHAS]
            
            if df_alagoinhas.empty:
                print(f"Nenhum dado de Tuberculose para Alagoinhas em {year}.")
                continue
                
            # 4. Preparação do arquivo
            local_filename = f"tuberculose_alagoinhas_{year}.csv"
            df_alagoinhas.to_csv(local_filename, index=False, sep=';', encoding='utf-8')
            
            # 5. Upload
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{local_filename}")
            blob.upload_from_filename(local_filename)
            print(f"Sucesso! Arquivo {local_filename} enviado para {DESTINATION_FOLDER}/")
            
            # Limpeza local opcional para economizar espaço
            if os.path.exists(local_filename):
                os.remove(local_filename)
                
        except Exception as e:
            print(f"Erro ao processar o ano {year}: {e}")

if __name__ == "__main__":
    run_oda_pipeline_tube_history()

if __name__ == "__main__":
    run_oda_pipeline_tube()
