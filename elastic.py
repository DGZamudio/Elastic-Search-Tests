from elastic_transport import ObjectApiResponse
from connection import es

class ElasticSearchFuns:
    def __init__(self):
        self.es = es
        
    # Indices
    def index_exists(self, index: str) -> bool:
        # Funcion para verificar si un indice existe dentro de la conexion con elastic search
        return self.es.indices.exists(index=index)

    def create_index(self, index: str, mapping, shards=1, replicas=0):
        # Funcion para crear un indice dentro de la conexion con elastic search | Default 1 Shard y 0 replicas para desarrollo y pruebas
        self.es.indices.create(
            index=index,
            settings={
                "index": {
                    "number_of_shards": shards, # Numero de Shards (Numero de partes en las que se divide un indice) (Cada shard almacena una parte de un documento asi al momento de hacer una busqueda toma menos tiempo) 1 Shard para prueba o prototipo | Mas shards != mejor velocidad de busqueda
                    "number_of_replicas": replicas # Numero de replicas (Numero de replicas de el indice) (Las replicas son copias de los shards, estas permiten mayor disponibilidad y backup) 0 replicas para prueba o prototipo | Mas replicas = Escritura mas lenta
                }
            }
        )
        
        self.es.indices.put_mapping(index=index, body=mapping) # Crear mapping manualmente para trabajar con Dense Vectors
        
    def get_mapping(self, index):
        # Funcion para verificar el mapping de un indice 
        return self.es.indices.get_mapping(index=index)

    def delete_index(self, index: str):
        # Funcion para eliminar un indice dentro de la conexion con elastic search
        self.es.indices.delete(index=index, ignore_unavailable=True)
        
    
    # Documentos | Datos
    def add_doc(self, index: str, document: object) -> ObjectApiResponse:
        # Funcion para añadir documentos dentro de un indice de elastic search
        return self.es.index(
            index=index,
            body=document
        )
        
    def count_docs(self, index, q = {}) -> ObjectApiResponse:
        # Funcion que devuelve un response que contiene el numero de documentos contados, ya sea total o filtrados mediante un query
        return self.es.count(index=index, query=q)
    
    def doc_exists(self, index, doc_id) -> bool:
        # Funcion que devuelve si un documento existe o no
        return self.es.exists(index=index, id=doc_id)
    
    def get_complete_doc(self, index, doc_id) -> ObjectApiResponse:
        # Funcion que devuelve el documento completo mediante el id del mismo
        return self.es.get(index=index, id=doc_id)
    
    def update_doc(self, index, doc_id, params) -> ObjectApiResponse:
        # Funcion para cambiar los datos de un documento
        return self.es.update(
            index=index,
            id=doc_id,
            script=params
        )
        
    def delete_doc(self, index, doc_id) -> ObjectApiResponse:
        # Funcion que elimina un documento de un indice
        return self.es.delete(index=index, id=doc_id)
    
    # Bulk API - API para hacer multiples tareas en una sola conexion - Escencial para subida masiva o demas
    def bulk(self, operations):
        return self.es.bulk(operations=operations)
    
    # Search API | Busqueda de datos
    def search(self, index, **kwargs) -> ObjectApiResponse:
        # Funcion principal para hacer uso del API de busqueda de Elastic Search
        return self.es.search(
            index=index, # Esta funcion puede recibir varios tipos de referencia a un indice, entre ellos: indice exacto, multiples indices separados por comas, usando almohadillas index* o usando _all para buscar en todos
            **kwargs
            #q - Parametro de filtrado, usa lenguaje Lucene y es mas basico
            #query - Parametro de filtrado, se usa para busquedas mas complejas y estructuradas, usa el lenguaje Query DSL
            #timeout - Tiempo de espera maximo para una busqueda
            #size - Numero de documentos que retorna | 10 Default 10000 Max
            #from - Usado para paginacion | cuantos documentos se salta
            #aggregations - Calculos para los datos, max, min, average, count
        )