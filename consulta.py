#DECLARANDO BIBLIOTECAS NECESSARIAS PARA BUILD DE SCRIPT
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

#EXECUTANDO CONSULTA AO BIGQUERY.
query = """    
SELECT channelGrouping AS Agrupamento_de_Canais, 
 CONCAT(trafficsource.source, trafficsource.medium)
  AS Origem_midia,
  trafficsource.campaign AS Campanha,
  0 as Sessoes,
  0 as Usuarios, 
  0 as Transacoes,
  '0.00' as Receita

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _TABLE_SUFFIX BETWEEN '07' AND '09' 
  ORDER BY DATE DESC 
"""

#FUNCAO QUE GERA UM ARQUIVO .CSV 
def geraCSV(objeto, caminho):
    objeto.to_csv(caminho,index=False, encoding='utf-8', sep = ',') 

def executaConsulta(sql):
    # AQUI ESTOU CRIANDO UM OBJETO COM AS CREDENCIAIS
    credentials = service_account.Credentials.from_service_account_file('blinks-essence-319111-965ef6aefac6.json')
    project_id = 'blinks-essence-319111'
    
    # AQUI ESTOU INSTANCIANDO O OBJETO COM AS CREDENCIAIS E O PROJETO
    client = bigquery.Client(credentials= credentials,project=project_id)
    print(client) 
    
    # EXECUTANDO UMA QUERY NO BIGQUERY
    query = client.query(sql)
    
    #RETORNANDO OS RESULTADOS 
    results =  query.result()    
    df = results.to_dataframe()
    return df
   
   #INSTANCIANDO O ARQUIVO .CSV ATRIBUINDO AO MESMO RESULTADOS DA QUERY SEPARANDO OS DADOS POR VIRGULA
    # df.to_csv('teste.csv',index=False, encoding='utf-8', sep = ',') 
    # for row in results:
    #     print(row)
    # return results
    
#RETORNANDO UM OBJETO COM A CONSULTA
retorno = executaConsulta(query)

# METODO RETORNA O ARQUIVO .CSV
geraCSV(retorno, "blinks.csv")
print(retorno)

