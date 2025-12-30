from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200') # Conectar a instancia de elastic search local en docker
client_info = es.info() # Recolectar informacion de la conexion a ES (Elastic Search)

print('Conectado a ElasticSearch!')
pprint(client_info) # Imprimir informacion de la conexion junto con su formato