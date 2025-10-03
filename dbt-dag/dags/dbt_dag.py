import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Profil qui va utiliser la connexion snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_taxi_conn",
        profile_args={"database": "NYC_TAXI_DB", "schema":"RAW"} 
    )
)

# Projet DBT
project_config = ProjectConfig(
    "/usr/local/airflow/dags/dbt/taxi_nyc_pipeline"
)

# Création du DAG dbt
with DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    operator_args={"install_deps": True},  
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_staging_dag",
) as dag:

    # Task staging
    dbt_staging_task = dag.task(
        task_id="dbt_staging",
        dbt_command="run",
        dbt_command_args=["--select", "staging"]
    )

    # Task intermediate
    dbt_final_task = dag.task(
        task_id="dbt_intermediate",
        dbt_command="run",
        dbt_command_args=["--select", "intermediate"]
    )

    # Task marts
    dbt_final_task = dag.task(
        task_id="dbt_marts",
        dbt_command="run",
        dbt_command_args=["--select", "marts"]
    )