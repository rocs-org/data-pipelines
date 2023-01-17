from airflow.operators.bash import BashOperator
import ramda as R
import json


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
def create_dbt_task(
    dag, dbt_dir: str, dbt_verb: str, env: dict, node: str
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
        dbt {dbt_verb} --select {node} --models {model}
        """,
    )


@R.curry
def add_task_to_dbt_node(create_dbt_task, node_name, node):
    return {**node, "task": create_dbt_task(node_name)}


@R.curry
def map_over_dict(f, d):
    return {k: f(k, v) for k, v in d.items()}


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


def create_dbt_task_tree(
    dag, base_task, dbt_dir: str, dbt_verb: str, env: dict
) -> dict:
    return R.pipe(
        load_dbt_nodes_from_file,
        filter_dbt_nodes_for("model"),
        map_over_dict(
            add_task_to_dbt_node(create_dbt_task(dag, dbt_dir, dbt_verb, env))
        ),
        set_task_dependencies(base_task),
    )(dbt_dir + "/target/manifest.json")
