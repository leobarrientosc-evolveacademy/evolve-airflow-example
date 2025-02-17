from __future__ import annotations

import random
from datetime import datetime
from google.cloud import storage

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from operators.gcp import GenerativeModelGenerateImageOperator
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
    style = random.choice(styles)
    painters_of_the_style = painters_by_style[random.choice(styles)]
    painters = random.sample(painters_of_the_style, 2)
    kwargs['ti'].xcom_push(key='style', value=style)
    kwargs['ti'].xcom_push(key='painters', value=painters[:2])
  
def _generate_prompt(**kwargs):
    style = kwargs['ti'].xcom_pull(task_ids='generate_style_and_painters', key='style')
    painters = kwargs['ti'].xcom_pull(task_ids='generate_style_and_painters', key='painters')
    prompt =  "Generate an painting about %s like %s." % (style , " or ".join(painters))
    kwargs['ti'].xcom_push(key='prompt', value=prompt)
    # Create an MD5 hash of the prompt
    md5_hash = hashlib.md5(prompt.encode()).hexdigest()
    # Create the file path with the date and MD5 hash
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f"data/{date_str}/{md5_hash}.txt"
    # Save the prompt to a file
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        file.write(prompt)
    kwargs['ti'].xcom_push(key='prompt_file', value=file_path)


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

    task_save_prompt = LocalFilesystemToGCSOperator(
        task_id="save_prompt",
        gcp_conn_id="google_cloud_default",
        src="{{ ti.xcom_pull(task_ids='generate_prompt', key='prompt_file') }}",
        dst="{{ ti.xcom_pull(task_ids='generate_prompt', key='prompt_file') }}",
        bucket=os.getenv('GCS_BUCKET_NAME'),
        mime_type='text/plain',
    )

    task_generate_image = GenerativeModelGenerateImageOperator(
        task_id="generate_image",
        gcp_conn_id="google_cloud_default",
        project_id=os.getenv('GCP_PROJECT_ID'),
        prompt="{{ ti.xcom_pull(task_ids='generate_prompt', key='prompt') }}",
        location=os.getenv('GCP_PROJECT_LOCATION'),
        pretrained_model=os.getenv('GCP_PROJECT_PRETRAINED_MODEL'),
    )

    task_end = EmptyOperator(task_id="send")

    task_start >> task_generate_style_and_painters >> task_generate_prompt >> [task_save_prompt, task_generate_image] >> task_end