from __future__ import annotations

import random
from datetime import datetime
from google.cloud import aiplatform
from google.cloud import storage

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import hashlib
import os

# Dictionary of painting styles and painters
painters_by_style = {
    "Baroque": ["Caravaggio", "Peter Paul Rubens", "Rembrandt", "Johannes Vermeer", "Diego Velázquez"],
    "Renaissance": ["Leonardo da Vinci", "Michelangelo", "Raphael", "Sandro Botticelli", "Titian"],
    "Romanticism": ["Caspar David Friedrich", "Eugène Delacroix", "J.M.W. Turner", "Francisco Goya", "John Constable"],
    "Realism": ["Gustave Courbet", "Jean-François Millet", "Honoré Daumier", "Rosa Bonheur", "Ilya Repin"],
    "Abstract Expressionism": ["Jackson Pollock", "Mark Rothko", "Willem de Kooning", "Franz Kline", "Barnett Newman"],
    "Impressionism": ["Claude Monet", "Edgar Degas", "Pierre-Auguste Renoir", "Mary Cassatt", "Camille Pissarro"],
    "Post-Impressionism": ["Vincent van Gogh", "Paul Cézanne", "Paul Gauguin", "Henri de Toulouse-Lautrec", "Georges Seurat"],
    "Expressionism": ["Edvard Munch", "Ernst Ludwig Kirchner", "Emil Nolde", "Franz Marc", "Egon Schiele"],
    "Cubism": ["Pablo Picasso", "Georges Braque", "Juan Gris", "Fernand Léger", "Robert Delaunay"],
    "Surrealism": ["Salvador Dalí", "René Magritte", "Joan Miró", "Max Ernst", "André Breton"],
    "Pop Art": ["Andy Warhol", "Roy Lichtenstein", "James Rosenquist", "Claes Oldenburg", "Robert Rauschenberg"],
    "Minimalism": ["Donald Judd", "Sol LeWitt", "Agnes Martin", "Dan Flavin", "Robert Morris"],
    "Contemporary Art": ["Jeff Koons", "Damien Hirst", "Ai Weiwei", "Marina Abramović", "Takashi Murakami"]
}

def _generate_style_and_painters(**kwargs):
    """
    Randomly selects a painting style and two painters.
    """
    styles = list(painters_by_style.keys())
    painters_of_the_style = painters_by_style[random.choice(styles)]
    painters = random.sample(painters_of_the_style, 2)
    kwargs['ti'].xcom_push(key='estilo', value=styles)
    kwargs['ti'].xcom_push(key='pintor_1', value=painters[0])
    kwargs['ti'].xcom_push(key='pintor_2', value=painters[1])

def _generate_prompt(**kwargs):
    estilo = kwargs['ti'].xcom_pull(task_ids='obtener_estilo_y_pintores', key='estilo')
    pintor_1 = kwargs['ti'].xcom_pull(task_ids='obtener_estilo_y_pintores', key='pintor_1')
    pintor_2 = kwargs['ti'].xcom_pull(task_ids='obtener_estilo_y_pintores', key='pintor_2')
    prompt =  "Generate an painting about " + estilo + " like " + pintor_1 + " or " + pintor_2 + "."
    kwargs['ti'].xcom_push(key='prompt', value=prompt)

def _save_prompt(**kwargs):
    """
    Save a generated prompt to Google Cloud Storage (GCS).
    This function retrieves a prompt from Airflow's XCom, generates an MD5 hash of the prompt,
    and saves it to a GCS bucket with a file path that includes the current date and the MD5 hash.
    Args:
        **kwargs: Arbitrary keyword arguments passed by Airflow. Expected to contain:
            - ti: TaskInstance object from which XComs can be pulled.
    Environment Variables:
        GCP_PROJECT_ID: The Google Cloud Project ID.
        GCS_BUCKET_NAME: The name of the GCS bucket where the prompt will be saved.
    Raises:
        Exception: If there is an error uploading the prompt to GCS.
    Example:
        _save_prompt(ti=task_instance)
    """

    prompt = kwargs['ti'].xcom_pull(task_ids='generate_prompt', key='prompt')
    project_id = os.getenv('GCP_PROJECT_ID')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    
    # Generate MD5 hash of the prompt
    md5_hash = hashlib.md5(prompt.encode()).hexdigest()
    
    # Create the file path with the date and MD5 hash
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f"data/{date_str}/{md5_hash}.txt"
    
    # Initialize the GCS client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Upload the prompt to GCS
    try:
        blob.upload_from_string(prompt)
        kwargs['ti'].log.info(f"Prompt saved to gs://{bucket_name}/{file_path}")
    except Exception as e:
        kwargs['ti'].log.error(f"Error saving prompt to GCS: {e}")
        raise

def _generate_image(**kwargs):
    """
    Genera una imagen utilizando Vertex AI Vision y la almacena en un bucket de GCS.

    Args:
        prompt: El prompt para la generación de la imagen.
        bucket_name: El nombre del bucket de GCS.
        nombre_archivo: El nombre del archivo para la imagen en el bucket.
    """
    prompt = kwargs['ti'].xcom_pull(task_ids='generate_prompt', key='prompt')
    bucket_name = "us-central1-airflow-001-9fbf403d-bucket"
    nombre_archivo = "data/%s.png" 

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
    dag_id="random_style_and_painters",
    start_date=datetime(2023, 10, 26),
    schedule=None,
    catchup=False,
) as dag:
    
    task_start = EmptyOperator(task_id="start")

    task_generate_style_and_painters = PythonOperator(
        task_id="generate_style_and_painters",
        python_callable=_generate_style_and_painters,
        provide_context=True,
    )

    task_generate_prompt = PythonOperator(
        task_id="generate_prompt",
        python_callable=_generate_prompt,
        provide_context=True,
    )

    task_save_prompt = PythonOperator(
        task_id="save_prompt",
        python_callable=_save_prompt,
        provide_context=True
    )

    task_generate_image = PythonOperator(
       task_id="generate_image",
       python_callable=_generate_image,
       provide_context=True
    )

    task_end = EmptyOperator(task_id="send")

    task_start >> task_generate_style_and_painters >> task_generate_prompt >> task_save_prompt >> task_generate_image >> task_end