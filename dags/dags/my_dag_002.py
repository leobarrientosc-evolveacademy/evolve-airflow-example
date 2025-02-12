from __future__ import annotations

import random
from datetime import datetime

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

    end = EmptyOperator(task_id="fin")

    start >> tarea_estilo_y_pintores >> tarea_prompt >> tarea_guardar_prompt >> end