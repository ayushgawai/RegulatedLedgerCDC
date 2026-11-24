import importlib.util
from pathlib import Path


def test_dag_task_graph_is_acyclic_and_ordered():
    path = Path(__file__).parents[1] / "dags" / "ledger_cdc_dag.py"
    spec = importlib.util.spec_from_file_location("ledger_cdc_dag", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    graph = mod.TASK_GRAPH
    assert list(graph) == ["capture_batch", "land_bitemporal", "reconcile",
                           "observability_checks", "publish_health"]
    # acyclic: topological walk terminates visiting every node once
    seen, stack = [], list(graph)
    order = ["capture_batch", "land_bitemporal", "reconcile",
             "observability_checks", "publish_health"]
    for node in order:
        assert graph[node] == ([order[order.index(node) + 1]] if node != order[-1] else [])
        seen.append(node)
    assert len(seen) == len(set(seen)) == 5
    assert mod.dag.dag_id == "regulated_ledger_cdc" and mod.dag.catchup is False
