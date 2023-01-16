from src.lib.dag_helpers.dbt_helpers import (
    load_dbt_nodes_from_file,
    create_dbt_task,
    filter_dbt_nodes_for,
    add_task_to_dbt_node,
    map_over_dict,
    order_tasks_by_dependencies,
    create_dbt_task_tree,
)
from airflow.operators.bash import BashOperator
from airflow import DAG
import ramda as R


def test_load_dbt_nodes_from_file():
    """Test that we can load dbt nodes from a file"""

    nodes = load_dbt_nodes_from_file("dbt/target/manifest.json")
    assert len(nodes.keys()) > 3
    assert "model.data_pipelines.fifty_six_days" in nodes.keys()


def test_filter_dbt_nodes_for_models_returns_only_model_nodes():
    """Test that filter_dbt_nodes_for_models returns only model nodes"""
    nodes = load_dbt_nodes_from_file("dbt/target/manifest.json")
    model_nodes = filter_dbt_nodes_for("model", nodes)
    assert len(model_nodes) > 0
    for node in model_nodes.keys():
        assert node.split(".")[0] == "model"


def test_create_dbt_task_returns_bash_operator_task():
    """Test that create_dbt_task returns a task"""
    dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": "2021-01-01"})

    task = create_dbt_task(
        dag, "/opt/airflow/dbt/", "run", {"TARGET_DB_SCHEMA": "test"}
    )(
        "model.data_pipelines.user_vital_don_stats",
    )
    assert isinstance(task, BashOperator)

    # execute task
    task.execute({})


def test_create_all_dbt_tasks():
    dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": "2021-01-01"})
    nodes_with_task = R.pipe(
        load_dbt_nodes_from_file,
        filter_dbt_nodes_for("model"),
        map_over_dict(
            add_task_to_dbt_node(
                create_dbt_task(
                    dag, "/opt/airflow/dbt/", "run", {"TARGET_DB_SCHEMA": "test"}
                )
            )
        ),
    )("dbt/target/manifest.json")

    assert len(nodes_with_task) > 0
    for node in nodes_with_task.values():
        assert isinstance(node["task"], BashOperator)


def test_create_all_tasks_and_order_by_dependencies():
    dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": "2021-01-01"})
    tasks = R.pipe(
        load_dbt_nodes_from_file,
        filter_dbt_nodes_for("model"),
        map_over_dict(
            add_task_to_dbt_node(
                create_dbt_task(
                    dag, "/opt/airflow/dbt/", "run", {"TARGET_DB_SCHEMA": "test"}
                )
            )
        ),
    )("dbt/target/manifest.json")

    ordered_tasks = order_tasks_by_dependencies(None, tasks)

    assert len(ordered_tasks) > 0


def test_create_task_tree_sets_base_task():
    dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": "2021-01-01"})

    base_task = BashOperator(
        dag=dag,
        task_id="base_task",
        bash_command="echo 'base task'",
    )

    task_tree = create_dbt_task_tree(
        dag,
        base_task,
        "/opt/airflow/dbt/",
        "run",
        {"TARGET_DB_SCHEMA": "test"},
    )
    assert len(task_tree) > 0
    print(task_tree["model.data_pipelines.user_vital_don_stats"]["task"].upstream_list)
    assert False
