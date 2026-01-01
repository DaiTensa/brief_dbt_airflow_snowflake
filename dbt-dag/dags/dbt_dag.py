import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Profil qui va utiliser la connexion snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_taxi_conn", # Match airflow_settings.yaml
        profile_args={"database": "NYC_TAXI_DB", "schema": "RAW"}
    )
)

# Projet DBT
project_config = ProjectConfig(
    dbt_project_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/taxi_nyc_pipeline",
)

# Création du DAG dbt
with DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    # Render config to select specific models if needed, or default to all
    render_config=RenderConfig(
        select=["path:models/staging", "path:models/intermediate", "path:models/marts"]
    ),
    schedule_interval=None,
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_transformation_dag",
    tags=["dbt", "snowflake"],
) as dag:
    pass
    # Cosmos automatically generates tasks based on the dbt project structure