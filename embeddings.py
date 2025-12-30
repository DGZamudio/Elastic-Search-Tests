from sentence_transformers import SentenceTransformer
from bs4 import BeautifulSoup

model = SentenceTransformer("all-MiniLM-L6-v2")

def limpiar_html(texto):
    soup = BeautifulSoup(texto, "html.parser")
    return soup.get_text(separator=" ")

def get_embedding(texto):
    texto_limpio = limpiar_html(texto)

    embedding = model.encode(texto_limpio)
    return embedding.tolist()