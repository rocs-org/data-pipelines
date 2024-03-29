import requests
import json
import os
import ramda as R
from typing import Callable
from airflow.utils.context import Context


MessageCreator = Callable[[Context], str]
SlackNotifier = Callable[[Context], None]


def slack_notifier_factory(message_creator: MessageCreator) -> SlackNotifier:
    return R.pipe(
        message_creator,
        notify_slack(
            os.environ["SLACK_WEBHOOK_URL"],
        ),
    )


@R.curry
def notify_slack(webhook_url: str, message: str) -> None:
    requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"text": message}),
    )


def create_slack_error_message_from_task_context(context: Context) -> str:
    return """
        :red_circle: Task Failed.\n
        *Task*: {task}\n
        *Dag*: {dag} \n
        *Execution Time*: {exec_date}\n
        *Log Url*: <{log_url}|open here> - don't forget to connect to VPN
        """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
