from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import tempfile
import os

def novo_arquivo(event, context):
  bucket_name = event['bucket']
  file_name = event['name']
  st_client = storage.Client()
  bq_client = bigquery.Client()

  #Leitura do Excel
  print(f"Iniciando leitura do arquivo: {file_name}.")
  bucket = st_client.get_bucket(bucket_name)
  blob = bucket.blob(file_name)
  temp_file_path = os.path.join(tempfile.gettempdir(), file_name)
  blob.download_to_filename(temp_file_path)
  print(f"Download do arquivo temporário: {temp_file_path}")

  #Excel para Data Frame
  print(f"Carregando Data Frame: {file_name}")
  df = pd.read_excel(temp_file_path)
  print("Data Frame carregado com sucesso")   

  #Bulk Load na TMP                    
  j_load = bq_client.load_table_from_dataframe(df, "atomic-segment-386121.VD.TMP_VENDA", job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
  r_load = j_load.result()
  print("Dados inseridos na tabela TMP_VENDA")

  #Deleta registros 
  q_cmd = bq_client.query("DELETE FROM atomic-segment-386121.VD.VENDA A WHERE A.DATA_VENDA IN (SELECT DISTINCT B.DATA_VENDA FROM atomic-segment-386121.VD.TMP_VENDA B)")
  r_cmd = q_cmd.result()
  print("Dados antigos apagados da VENDA")

  #Insert na Tabela VENDA
  q_cmd = bq_client.query("INSERT INTO atomic-segment-386121.VD.VENDA (ID_MARCA, MARCA, ID_LINHA,LINHA,DATA_VENDA,QTD_VENDA) SELECT ID_MARCA, MARCA, ID_LINHA,LINHA,DATA_VENDA,SUM(QTD_VENDA) AS QTD_VENDA FROM atomic-segment-386121.VD.TMP_VENDA GROUP BY ID_MARCA, MARCA, ID_LINHA,LINHA,DATA_VENDA")
  r_cmd = q_cmd.result()
  print("Dados AGREGADOS e INSERIDOS na VENDA")

  #Atualização das Views Materilizadas
  q_cmd = bq_client.query("CALL BQ.REFRESH_MATERIALIZED_VIEW('VD.TB1_VENDA_ANO_MES'); CALL BQ.REFRESH_MATERIALIZED_VIEW('VD.TB2_MARCA_LINHA'); CALL BQ.REFRESH_MATERIALIZED_VIEW('VD.TB3_MARCA_ANO_MES');CALL BQ.REFRESH_MATERIALIZED_VIEW('VD.TB4_LINHA_ANO_MES');")
  r_cmd = q_cmd.result()
  print("Views Materializadas atualizadas")
  
  #Exclusão do arquivo temporário
  blob.delete()
  os.remove(temp_file_path)
  print(f"Arquivo temporário deletado: {temp_file_path}")
