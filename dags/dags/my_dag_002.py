from __future__ import annotations

import random
from datetime import datetime
from google.cloud import aiplatform
from google.cloud import storage

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Diccionario de estilos de pintura y pintores
pintores_por_estilo = {
    "Impressionism": ["Claude Monet", "Edgar Degas", "Pierre-Auguste Renoir", "Mary Cassatt", "Camille Pissarro"],
    "Post-Impressionism": ["Vincent van Gogh", "Paul Cézanne", "Paul Gauguin", "Henri de Toulouse-Lautrec", "Georges Seurat"],
    "Expressionism": ["Edvard Munch", "Ernst Ludwig Kirchner", "Emil Nolde", "Franz Marc", "Egon Schiele"],
    "Cubism": ["Pablo Picasso", "Georges Braque", "Juan Gris", "Fernand Léger", "Robert Delaunay"],
    "Surrealism": ["Salvador Dalí", "René Magritte", "Joan Miró", "Max Ernst", "André Breton"],
    "Pop Art": ["Andy Warhol", "Roy Lichtenstein", "James Rosenquist", "Claes Oldenburg", "Robert Rauschenberg"],
    "Minimalism": ["Donald Judd", "Sol LeWitt", "Agnes Martin", "Dan Flavin", "Robert Morris"],
    "Contemporary Art": ["Jeff Koons", "Damien Hirst", "Ai Weiwei", "Marina Abramović", "Takashi Murakami"]
}

def _obtener_pintores_y_estilo(**kwargs):
    """
    Selecciona aleatoriamente un estilo de pintura y dos pintores.
    """
    estilos = list(pintores_por_estilo.keys())
    estilo = random.choice(estilos)
    pintores_del_estilo = pintores_por_estilo[estilo]
    pintores = random.sample(pintores_del_estilo, 2)
    kwargs['ti'].xcom_push(key='estilo', value=estilo)
    kwargs['ti'].xcom_push(key='pintor_1', value=pintores[0])
    kwargs['ti'].xcom_push(key='pintor_2', value=pintores[1])

def _prompt(**kwargs):
    estilo = kwargs['ti'].xcom_pull(task_ids='obtener_estilo_y_pintores', key='estilo')
    pintor_1 = kwargs['ti'].xcom_pull(task_ids='obtener_estilo_y_pintores', key='pintor_1')
    pintor_2 = kwargs['ti'].xcom_pull(task_ids='obtener_estilo_y_pintores', key='pintor_2')
    prompt =  "Generate an painting about " + estilo + " like " + pintor_1 + " or " + pintor_2 + "."
    kwargs['ti'].xcom_push(key='prompt', value=prompt)

def _guardar_texto_en_fichero(**kwargs):
  
  nombre_fichero = "prompts.txt"
  texto = kwargs['ti'].xcom_pull(task_ids='obtener_prompt', key='prompt')
  """
  Guarda una cadena de texto en un fichero.

  Args:
    nombre_fichero: El nombre del fichero en el que se guardará el texto.
    texto: La cadena de texto que se va a guardar.
  """
  try:
    with open(nombre_fichero, 'w') as fichero:
      fichero.write(texto)
    print(f"El texto se ha guardado correctamente en {nombre_fichero}")
  except Exception as e:
    print(f"Ha ocurrido un error al guardar el texto: {e}")


def _generar_imagen(**kwargs):
    """
    Genera una imagen utilizando Vertex AI Vision y la almacena en un bucket de GCS.

    Args:
        prompt: El prompt para la generación de la imagen.
        bucket_name: El nombre del bucket de GCS.
        nombre_archivo: El nombre del archivo para la imagen en el bucket.
    """

    # Ejemplo de uso
    prompt = kwargs['ti'].xcom_pull(task_ids='obtener_prompt', key='prompt')
    bucket_name = "us-central1-airflow-001-9fbf403d-bucket"
    nombre_archivo = "data/imagen_generada.png"

    # Inicializa el cliente de Vertex AI
    aiplatform.init(project="airflow-at-gcp", location="us-west1")

    # Define el modelo de generación de imágenes (ajusta según tus necesidades)
    model_name = "imagen-3.0-generate-002"
    model = aiplatform.Model(model_name)

    # Crea la solicitud de generación de imagen
    response = model.predict(prompt=prompt)

    # Obtiene la imagen generada
    image_bytes = response.predictions[0].image_bytes

    # Inicializa el cliente de GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(nombre_archivo)

    # Sube la imagen al bucket
    blob.upload_from_string(image_bytes, content_type="image/png")  # Ajusta el tipo de contenido si es necesario

    print(f"Imagen generada y almacenada en gs://{bucket_name}/{nombre_archivo}")


with DAG(
    dag_id="estilo_y_pintores_aleatorios",
    start_date=datetime(2023, 10, 26),
    schedule=None,
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id="inicio")

    tarea_estilo_y_pintores = PythonOperator(
        task_id="obtener_estilo_y_pintores",
        python_callable=_obtener_pintores_y_estilo,
        provide_context=True,
    )

    tarea_prompt = PythonOperator(
        task_id="obtener_prompt",
        python_callable=_prompt,
        provide_context=True,
    )

    tarea_guardar_prompt = PythonOperator(
        task_id="guardar_prompt",
        python_callable=_guardar_texto_en_fichero,
        provide_context=True
    )

    tarea_generar_imagen = PythonOperator(
       task_id="generar_imagen",
       python_callable=_generar_imagen,
       provide_context=True
    )

    end = EmptyOperator(task_id="fin")

    start >> tarea_estilo_y_pintores >> tarea_prompt >> tarea_guardar_prompt >> tarea_generar_imagen >> end