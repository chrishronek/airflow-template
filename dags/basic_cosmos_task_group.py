"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag

from cosmos import DbtTaskGroup, ExecutionConfig, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.config import LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

from dags.utils.dbt.custom_operators import DbtDocsStupidOperator, DbtFreshnessStupidOperaotr

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "public"},
    ),
)

aws_bucket = "test"
dbt_binary = "/opt/airflow/dbt_venv/bin/dbt"

@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group() -> None:
    """
    The simplest example of using Cosmos to render a dbt project as a TaskGroup.
    """

    jaffle_shop = DbtTaskGroup(
        project_config=ProjectConfig(
            (DBT_ROOT_PATH / "jaffle_shop").as_posix(),
            project_name="jaffle_shop"
        ),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=dbt_binary
        ),
        render_config=RenderConfig(
            load_method=LoadMode.CUSTOM
        )
    )

    docs = DbtDocsStupidOperator(
        task_id="dbt_docs",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        dbt_executable_path=dbt_binary        
    )

    freshness = DbtFreshnessStupidOperaotr(
        task_id="dbt_freshness",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        dbt_executable_path=dbt_binary        
    )

    jaffle_shop >> [docs, freshness]


basic_cosmos_task_group()