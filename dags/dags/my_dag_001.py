from __future__ import annotations

import pendulum
import random

from datetime import timedelta
from time import sleep

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def _print_hello():
    print("Hello from the Python task!")

def _print_numeros_primos():
    print("Voy a escribir X numeros primos!.")
    limite = random.randint(1, 50)
    primos = []
    print("El límite es: " + str(limite) )
    for num in range(2, limite + 1):
      es_primo = True
      if num <= 1:
          es_primo = False
      else:
          for i in range(2, int(num**0.5) + 1):
              if num % i == 0:
                  es_primo = False
                  break
      if es_primo:
          print(" >>>> " + str(num))
          primos.append(num)
      sleep(1)

def decidir_adios():
    if random.random() < 0.5:
        return 'adios_en_ingles'
    else:
        return 'adios_en_espannol'
            
def _print_goodbye_on_english():
    print("Goodbye from the Python task!")

def _print_goodbye_on_castellano():
    print("Goodbye from the Python task!")

with DAG(
    dag_id="my_dag_001",
    schedule="*/1 * * * *",  # Run on demand
    start_date=pendulum.datetime(2025, 2, 10, tz="UTC"),
    catchup=True,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="inicio")

    hello_task = PythonOperator(
        task_id="hola",
        python_callable=_print_hello,
    )

    prime_number = PythonOperator(
        task_id="calculadora_de_numeros_primos",
         python_callable=_print_numeros_primos,
         execution_timeout=timedelta(seconds=10),
         trigger_rule="always"
    )

    tarea_branch = BranchPythonOperator(
        task_id='tarea_branch',
        python_callable=decidir_adios,
        dag=dag
    )

    goodbye_task = PythonOperator(
        task_id="adios_en_ingles",
        python_callable=_print_goodbye_on_english,
    )

    adios_task = PythonOperator(
        task_id="adios_en_espannol",
        python_callable=_print_goodbye_on_castellano,
    )

    end = EmptyOperator(task_id="fin", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> hello_task >> prime_number >> tarea_branch >> [goodbye_task, adios_task] >> end
