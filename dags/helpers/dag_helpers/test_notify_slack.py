import responses
import json
import os
from dags.helpers.dag_helpers.notify_slack import (
    notify_slack,
    slack_notifier_factory,
    create_slack_error_message_from_task_context,
)

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from dags.helpers.test_helpers import get_task_context


@responses.activate
def test_notify_slack_posts_message_to_url():
    responses.add(responses.POST, WEBHOOK_URL, status=200)

    notify_slack(WEBHOOK_URL, MESSAGE)
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == WEBHOOK_URL
    assert responses.calls[0].request.body == json.dumps({"text": MESSAGE})


@responses.activate
def test_notifier_factory_creates_notifier_with_correct_url_from_env():
    os.environ["SLACK_WEBHOOK_URL"] = WEBHOOK_URL
    responses.add(responses.POST, WEBHOOK_URL, status=200)
    context = get_task_context(DAG_ID, TASK_ID)
    notifier = slack_notifier_factory(create_slack_error_message_from_task_context)
    notifier(context)
    assert len(responses.calls) == 1


DAG_ID = "test_dag"
TASK_ID = "fail"
WEBHOOK_URL = "https://some.webhook.url/T2068U60P/B02GGBM9C64/TI3hOsUQ2VLzwuM2GEUH8jq1"
MESSAGE = "HELLO WORLD"


def fail():
    raise Exception


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="an example DAG that downloads a csv and uploads it to postgres",
    start_date=days_ago(1),
    tags=["TEST DAG"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(task_id=TASK_ID, python_callable=fail, dag=dag, op_args=[""])
