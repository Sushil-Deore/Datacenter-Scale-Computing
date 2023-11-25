
# Importing packages

from airflow import DAG

from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator

from ETL_Scripts.transform import transform_data
from ETL_Scripts.load import load_data, load_fact_data
from datetime import datetime
import os

SOURCE_URL =  "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
CSV_TARGET_DIR = AIRFLOW_HOME + "/data/{{ ds }}/downloads"
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'

PQ_TARGET_DIR = AIRFLOW_HOME + "/data/{{ ds }}/processed"

with DAG(
    dag_id = "outcomes_dag",
    start_date = datetime(2023,11,1),
    schedule_interval = '@daily'

) as dag:
    # Operators

    extract = BashOperator (
        task_id = "extract",
        bash_command =f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}"
    )

    transform = PythonOperator (
    task_id = "transform",
    python_callable = transform_data,
    op_kwargs= {'source_csv': CSV_TARGET_FILE,
                'target_dir': PQ_TARGET_DIR}
    )

    load_outcometype_dim = PythonOperator (
    task_id = "load_outcometype_dim",
    python_callable = load_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/dim_outcome_types.parquet',
                'table_name': 'outcometypedim',
                'key': 'outcome_type_id'})

    load_animaltype_dim = PythonOperator (
    task_id = "load_animaltype_dim",
    python_callable = load_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/dim_animal_type.parquet',
                'table_name': 'animaltypedim',
                'key': 'animaltype_id'})

    load_breed_dim = PythonOperator (
    task_id = "load_breed_dim",
    python_callable = load_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/dim_breed.parquet',
                'table_name': 'breeddim',
                'key': 'breed_id'})

    load_sex_dim = PythonOperator (
    task_id = "load_sex_dim",
    python_callable = load_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/dim_sex.parquet',
                'table_name': 'sexdim',
                'key': 'sex_id'})
    
    load_color_dim = PythonOperator (
    task_id = "load_color_dim",
    python_callable = load_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/dim_color.parquet',
                'table_name': 'colordim',
                'key': 'color_id'})
    
    load_date_dim = PythonOperator (
    task_id = "load_date_dim",
    python_callable = load_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/dim_dates.parquet',
                'table_name': 'datedim',
                'key': 'date_id'})
    
    load_outcome_fact = PythonOperator (
    task_id = "load_outcome_fact",
    python_callable = load_fact_data,
    op_kwargs= {'table_file': PQ_TARGET_DIR+'/fact_outcomes.parquet',
                'table_name': 'adoption'})

    extract >> transform >> [load_outcometype_dim, load_animaltype_dim, load_breed_dim, load_sex_dim, load_color_dim, load_date_dim] >> load_outcome_fact
