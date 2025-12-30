import json
from elasticsearch import NotFoundError
from tqdm import tqdm
from pprint import pprint
from elastic_transport import ObjectApiResponse
from elastic import ElasticSearchFuns


class IndexService:
    def __init__(self, repo: ElasticSearchFuns):
        self.repo = repo
        self.current_index = None



    # Utils
    def _require_index(self):
        # Funcion para verificar si hay un indice seleccionado
        if not self.current_index:
            raise Exception("No hay índice seleccionado")
        
    def _require_doc(self, doc_id):
        # Funcion para verificar si un documento existe dentro de un indice
        self._require_index()
        if not self.repo.doc_exists(self.current_index, doc_id):
            raise Exception("No se encontro ningun documento con este id")
        
    def print_info(self, response:ObjectApiResponse):
        # Funcion para imprimir el resultado de inserciones de documentos
        print(f"""
          El documento {response["_id"]} fue {response["result"]}
        """)
        
    def create_update_str(data):
        # Funcion para crear el string que requiere el update_doc de elastic search, recibe un diccionario con los campos a cambiar como llaves y los datos como sus valores
        src=""
        prm={}
        
        for campo, dato in data:
            if "remove" in campo: # Al intentar remover un campo del documento usar dato como el nombre del campo y campo como remove('campo')
                src += f"ctx._source.{campo} = params.{dato};\n"
            else:
                src += f"ctx._source.{campo} = params.{campo};\n"
                prm["campo"] = dato
            
        return {"source":src, "params":prm}
    
    
    
    # Index management
    def create_index(self, index: str, overwrite=False):
        # Funcion para crear un indice en elastic search
        if self.repo.index_exists(index): # Verificamos si el indice existe
            if not overwrite: # Si no se desea sobreescribir el indice
                raise Exception("El índice ya existe")
            self.repo.delete_index(index) # Se elimina el indice si se desea

        self.repo.create_index(index)
        self.current_index = index # Se settea el indice actual

    def select_index(self, index: str):
        # Funcion para seleccionar un indice y trabajar con el
        if not self.repo.index_exists(index):
            raise Exception("El índice no existe")

        self.current_index = index
        
      
        
    # Data management
    def add_doc_es(self, mongo_id, id_noticia, tema, grupo, fecha, titulo, resumen, contenido_completo, tipo_norma, estado, fuente):
        # Funcion para añadir un documento dentro de elastic search
        self._require_index()
        
        document = {
            "mongo_id": mongo_id,
            "id_noticia": id_noticia,
            "tema": tema,
            "grupo": grupo,
            "fecha": fecha,
            "titulo": titulo,
            "resumen": resumen,
            "contenido_completo": contenido_completo,
            "tipo_norma": tipo_norma,
            "estado": estado,
            "fuente": fuente
        }
        
        response = self.repo.add_doc(index=self.current_index, document=document)
        
        self.print_info(response)
        
    def add_docs_es(self, file):
        # Funcion para subir multiples documentos mediante un archivo formato JSON
        self._require_index()
        try:
            data = json.load(open(file, "r", encoding="utf-8"))
            
            for documento in tqdm(data, total=len(data)):
                self.repo.add_doc(self.current_index, documento)
            print("Los documentos fueron insertados")
        except Exception as e:
            print(f"Error al abrir el documento: {e}")
            
    def count_docs_es(self, q = {}):
        # Esta funcion cuenta el numero de documentos dentro de un indice y si es necesario puede contar el numero de documentos filtrado por una query especifica
        self._require_index()
        
        response = self.repo.count_docs(self.current_index, q)
        numero = response["count"]
        
        print(f"El numero de documentos es {numero}")
        
        return numero
    
    def get_complete_doc_es(self, doc_id):
        # Funcion para obtener el documento completo, se usa la funcion get, esta funcion requiere minimo el indice donde esta el documento y la id exacta del documento
        self._require_doc(doc_id)
        try:
            response = self.repo.get_complete_doc(index=self.current_index, doc_id=doc_id)
            documento = response["_source"]
            
            pprint(f"Documento completo: \n{documento}")
            return documento
        except NotFoundError:
            print("El documento no fue encontrado")
        except Exception as e:
            print(f"Error al traer el documento completo: {e}")
            
    def update_doc_es(self, doc_id, data):
        # Funcion para actualizar un documento
        self._require_doc(doc_id)
        
        script = self.create_update_str(data)
        
        response = self.repo.update_doc(index=self.current_index, doc_id=doc_id, params=script)
        
        pprint(response.body)
        
    def delete_doc_es(self, doc_id):
        # Funcion para eliminar un documento
        self._require_doc(doc_id)
        
        response = self.repo.delete_doc(index=self.current_index, doc_id=doc_id)
        
        pprint(response.body)
    
    
    
    # Search API | Busqueda de datos
    def match_search(self, query={}):
        # Busqueda mediante query match, la cual trae los documentos que contienen un texto, numero, fecha o valor especifico dentro de un campo especifico - Busqueda de texto
        self._require_index()
        if len(query) > 0:
            response = self.repo.search(
                index=self.current_index, 
                query= {
                    "match":query
                }    
            )
        else:
            response = self.repo.search(
                index=self.current_index
            )
        
        pprint(response["hits"]["hits"])
        
    def term_search(self, query):
        # Busqueda mediante query term, la cual trae solo los documentos que contienen un valor exacto dentro de un campo exacto de los documentos - Busqueda de Keywords o valores unicos entre si
        self._require_index()
        response = self.repo.search(
            index=self.current_index,
            query={
                "term":query
            }
        )
        
        pprint(response["hits"]["hits"])
        
    def range_search(self, query):
        # Busqueda mediante query range, la cual trae solo los documentos que contienen rango de valores - Busqueda con rangos recomendado para fechas
        self._require_index()
        response = self.repo.search(
            index=self.current_index,
            query={
                "range":query
            }
        )
        
        pprint(response["hits"]["hits"])