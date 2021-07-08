import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

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

def geraCSV(objeto, caminho):
    objeto.to_csv(caminho,index=False, encoding='utf-8', sep = ',') 


def executaConsulta(sql):
    # AQUI ESTOU CRIANDO UM OBJETO COM AS CREDENCIAIS
    credentials = service_account.Credentials.from_service_account_file('blinks-essence-319111-965ef6aefac6.json')
    project_id = 'blinks-essence-319111'
    # AQUI ESTOU INSTANCIANDO O OBJETO COM AS CREDENCIAIS E O PROJETO
    client = bigquery.Client(credentials= credentials,project=project_id)
    print(client)   
    # EXECUTANDO QUERY NO CLOUD
    query = client.query(sql)
    #PEGANDOS OS RESULTADOS 
    results =  query.result()    
    df = results.to_dataframe()
    
    return df
    # df.to_csv('teste.csv',index=False, encoding='utf-8', sep = ',') 
    # for row in results:
    #     print(row)
    # return results

retorno = executaConsulta(query)
geraCSV(retorno, "blinks.csv")

print(retorno)
