from pprint import pprint
from embeddings import get_embedding
from service import ElasticService


def main():
    elastic = ElasticService()
    
    # # 1. Crear indice con mapping definido para embeddings
    # elastic.create_index(
    #     index='prueba',
    #     mapping={
    #         'properties':{
    #             'mongo_id': {
    #                 'type': 'keyword'
    #             },
    #             'id_noticia': {
    #                 'type': 'keyword'
    #             },
    #             'tema': {
    #                 'type': 'keyword'
    #             },
    #             'grupo': {
    #                 'type': 'keyword'
    #             },
    #             'fecha': {
    #                 'type': 'date'
    #             },
    #             'titulo': {
    #                 'type':'text',
    #                 'fields': {
    #                     'keyword': {
    #                         'type': 'keyword'
    #                     }
    #                 }
    #             },
    #             'resumen': {
    #                 'type':'text',
    #                 'fields': {
    #                     'keyword': {
    #                         'type': 'keyword',
    #                         'ignore_above': 256
    #                     }
    #                 }
    #             },
    #             'contenido_completo': {
    #                 'type': 'text'
    #             },
    #             'tipo_norma': {
    #                 'type': 'keyword'
    #             },
    #             'estado': {
    #                 'type': 'keyword'
    #             },
    #             'fuente': {
    #                 'type': 'keyword'
    #             },
    #             'embedding': {
    #                 'type': 'dense_vector',
    #                 'dims': 384,
    #                 'index': True,
    #                 'similarity': 'cosine'
    #             }
    #         }
    #     },
    #     overwrite=True
    # )
    
    # # 2. Insertar documentos de prueba para la busqueda
    # elastic.add_docs_es("./data/noticias_prueba.json")
    
    # Seleccionamos un indice
    elastic.select_index("prueba")
    
    # Verificamos el mapping del indice
    # elastic.get_mapping_index()
    
    # # 3. Verificar si estan los documentos
    # response = elastic.main_search()
    # print(f"Response: {response}")
    
    # 4. Hacer busquedas knn
    while True:
        texto = input("Ingresa el tema a buscar: ")
        documentos = elastic.main_search(
            _source=["titulo", "resumen", "fecha", "tema"],
            knn={
                'field':'embedding',
                'query_vector': get_embedding(texto),
                'num_candidates': 5, # Numero de documentos que analiza el algoritmo knn, elastic search por defecto trae los documentos que mas similitud tienen a la busqueda,
                'k': 3 # Numero de documentos que devuelve la busqueda
            }
        )
        
        pprint(documentos)

if __name__ == '__main__':
    main()