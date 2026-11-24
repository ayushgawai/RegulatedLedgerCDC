"""Airflow DAG definitions for RegulatedLedgerCDC orchestration.

Importable without Airflow installed (a tiny stub is used), so the DAG
structure — task order, retries, schedule — can be unit-checked anywhere.
In a real deployment, drop this file into the Airflow DAG folder; the
operators below shell out to the ``ledger-cdc`` CLI.

Schedule: hourly batch with catchup disabled; reconciliation or a
CRITICAL observability alert fails the run (non-zero CLI/API gate is a
deployment-side wrapper around the same Pipeline.health() payload).
"""
from __future__ import annotations

try:  # pragma: no cover - Airflow is an optional deployment dependency
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    HAS_AIRFLOW = True
except ImportError:  # pragma: no cover
    HAS_AIRFLOW = False

    class DAG:  # minimal stub mirroring the context-manager API
        def __init__(self, dag_id, schedule=None, catchup=True, default_args=None, **kw):
            self.dag_id, self.schedule, self.catchup = dag_id, schedule, catchup
            self.default_args = default_args or {}
            self.tasks = []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class BashOperator:  # type: ignore
        def __init__(self, task_id, bash_command, dag=None, **kw):
            self.task_id, self.bash_command = task_id, bash_command
            self.downstream: list = []
            if dag is not None:
                dag.tasks.append(self)

        def __rshift__(self, other):
            self.downstream.append(other)
            return other


DEFAULT_ARGS = {"owner": "data-platform", "retries": 2, "retry_delay_seconds": 300}

# Declarative view of the flow (asserted in tests, independent of Airflow).
TASK_GRAPH = {
    "capture_batch": ["land_bitemporal"],
    "land_bitemporal": ["reconcile"],
    "reconcile": ["observability_checks"],
    "observability_checks": ["publish_health"],
    "publish_health": [],
}


def build_dag():
    with DAG("regulated_ledger_cdc", schedule="@hourly", catchup=False,
             default_args=DEFAULT_ARGS) as dag:
        capture = BashOperator(task_id="capture_batch", bash_command="ledger-cdc batch", dag=dag)
        land = BashOperator(task_id="land_bitemporal", bash_command="ledger-cdc batch", dag=dag)
        reconcile = BashOperator(task_id="reconcile", bash_command="ledger-cdc health", dag=dag)
        observe = BashOperator(task_id="observability_checks", bash_command="ledger-cdc health", dag=dag)
        publish = BashOperator(task_id="publish_health", bash_command="ledger-cdc health", dag=dag)
        capture >> land >> reconcile >> observe >> publish
    return dag


dag = build_dag()
