import json
import os

import ramda as R
from airflow.models import DAG
from airflow.operators.bash import BashOperator


def create_dbt_task_tree(
    dag, base_task, dbt_dir: str, dbt_verb: str, env: dict
) -> dict:

    # inject test db into env.
    # This is needed for the dbt tests to run but is convoluted and hacky because:
    # To get the test db, we need to get the test db from the dag config. This is done via jinja templating.
    # To default to the production db when not running tests, we need to access the actual env in jinja.
    # To do this, we need to use jinja filters that have to be set to the dag objects templating environment.
    def default_to_env_var(input, env_var_name):
        if input:
            return input
        return os.environ.get(env_var_name)

    try:
        dag.user_defined_filters["default_to_env_var"] = default_to_env_var
    except TypeError:
        dag.user_defined_filters = {"default_to_env_var": default_to_env_var}

    env[
        "TARGET_DB"
    ] = "{{ dag_run.conf.get('TARGET_DB') | default_to_env_var('TARGET_DB') }}"

    # do the actual dbt stuff
    return R.pipe(
        load_dbt_nodes_from_file,
        filter_dbt_nodes_for("model"),
        map_over_dict(
            add_task_to_dbt_node(create_dbt_task(dag, dbt_dir, dbt_verb, env))
        ),
        set_task_dependencies(base_task),
    )(dbt_dir + "/target/manifest.json")


def load_dbt_nodes_from_file(path: str) -> dict:
    """Load dbt nodes from a file"""
    with open(path) as f:
        nodes = json.load(f)
    return nodes["nodes"]


@R.curry
def filter_dbt_nodes_for(node_type: str, nodes: dict) -> dict:
    """Filter dbt nodes for a specific type"""
    return {k: v for k, v in nodes.items() if k.split(".")[0] == node_type}


@R.curry
def map_over_dict(f, d):
    return {k: f(k, v) for k, v in d.items()}


@R.curry
def add_task_to_dbt_node(create_dbt_task, node_name, node):
    return {**node, "task": create_dbt_task(node_name)}


@R.curry
def create_dbt_task(
    dag: DAG, dbt_dir: str, dbt_verb: str, env: dict, node: str
) -> BashOperator:
    """Create a task that runs a dbt command"""

    model = node.split(".")[-1]

    return BashOperator(
        dag=dag,
        task_id=f"dbt_{dbt_verb}_{node}",
        append_env=True,
        env=env,
        bash_command=f"""
        cd {dbt_dir} &&
        echo "Target DB: $TARGET_DB" &&
        dbt {dbt_verb} --select {node} --models {model}
        """,
    )


@R.curry
def set_task_dependencies(base_task, nodes: dict) -> dict:
    """Order tasks by dependencies"""
    for node_name, node in nodes.items():
        upstream_models = [
            n for n in node["depends_on"]["nodes"] if n.split(".")[0] == "model"
        ]
        if len(upstream_models) == 0 and base_task is not None:
            node["task"].set_upstream(base_task)
        for dep in upstream_models:
            node["task"].set_upstream(nodes[dep]["task"])
    return nodes
