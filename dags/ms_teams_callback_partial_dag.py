"""
### Success and Failure Callbacks with Partial

Shows how success and failures callbacks can be used with the partial module.

Sends a notification to Micorosft teams in different channels.
"""


import datetime
from datetime import timedelta
from functools import partial

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from include import ms_teams_callback_functions_with_partial

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    # Callbacks set in default_args will apply to all tasks unless overridden at the task-level
    # Using the partial module allows you to specify different channels for different alerts
    "on_success_callback": partial(
        ms_teams_callback_functions_with_partial.success_callback,
        http_conn_id="ms_teams_callbacks_partial",
    ),
    "on_failure_callback": partial(
        ms_teams_callback_functions_with_partial.failure_callback,
        http_conn_id="ms_teams_callbacks_partial",
    ),
    "on_retry_callback": partial(
        ms_teams_callback_functions_with_partial.retry_callback,
        http_conn_id="ms_teams_callbacks_partial",
    ),
    "sla": timedelta(seconds=10),
}

with DAG(
    dag_id="ms_teams_callbacks_partial",
    default_args=default_args,
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=2),
    # sla_miss only applies to scheduled DAG runs, it does not work for manually triggered runs
    # If a DAG is running for the first time and sla is missed, sla_miss will not fire on that first run
    sla_miss_callback=partial(
        ms_teams_callback_functions_with_partial.sla_miss_callback,
        http_conn_id="ms_teams_callbacks_partial",
    ),
    catchup=False,
    doc_md=__doc__,
) as dag:
    # This task uses on_execute_callback set in the default_args to send a notification when the task begins
    # and overrides the on_success_callback to None
    dummy_dag_triggered = DummyOperator(
        task_id="dummy_dag_triggered",
        on_execute_callback=partial(
            ms_teams_callback_functions_with_partial.dag_triggered_callback,
            http_conn_id="ms_teams_callbacks_partial",
        ),
        on_success_callback=None,
    )

    # This task uses the default_args on_success_callback
    dummy_task_success = DummyOperator(
        task_id="dummy_task_success",
    )

    # This task sends a Slack message via a python_callable
    ms_teams_python_op = PythonOperator(
        task_id="ms_teams_python_op",
        python_callable=partial(
            ms_teams_callback_functions_with_partial.python_operator_callback,
            http_conn_id="ms_teams_callbacks_partial",
        ),
        on_success_callback=None,
    )

    # Task will sleep beyond the 10 second SLA to showcase sla_miss callback
    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",
    )

    # Task will retry once before failing to showcase on_retry_callback and on_failure_callback
    bash_fail = BashOperator(
        task_id="bash_fail",
        retries=1,
        bash_command="exit 123",
    )

    # Task will still succeed despite previous task failing, showcasing use of the
    # last task in a DAG to notify that the DAG has completed
    dummy_dag_success = DummyOperator(
        task_id="dummy_dag_success",
        on_success_callback=partial(
            ms_teams_callback_functions_with_partial.dag_success_callback,
            http_conn_id="ms_teams_callbacks_partial",
        ),
        trigger_rule="all_done",
    )

    (
        dummy_dag_triggered
        >> dummy_task_success
        >> ms_teams_python_op
        >> bash_sleep
        >> bash_fail
        >> dummy_dag_success
    )
