"""CLI: run a batch, stream capture, check health, benchmark, serve."""
from __future__ import annotations

import argparse
import json
import sys

from .benchmark import run_benchmark
from .pipeline import Pipeline
from .source import seed_demo_source


def _pipeline(args) -> Pipeline:
    source = seed_demo_source(args.events)
    return Pipeline(source, workdir=args.workdir)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="ledger-cdc")
    parser.add_argument("--workdir", default="data/demo")
    parser.add_argument("--events", type=int, default=20, help="synthetic source size")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("batch", help="run one capture+land+reconcile batch")
    stream = sub.add_parser("stream", help="stream micro-batches")
    stream.add_argument("--max-batches", type=int, default=5)
    stream.add_argument("--interval", type=float, default=0.0)
    sub.add_parser("health", help="print pipeline health (runs a batch first)")
    bench = sub.add_parser("bench", help="run benchmark")
    bench.add_argument("-n", "--bench-events", type=int, default=2000)
    serve = sub.add_parser("serve", help="run FastAPI service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    args = parser.parse_args(argv)

    if args.cmd == "bench":
        print(json.dumps(run_benchmark(args.bench_events), indent=2))
        return 0
    if args.cmd == "serve":
        import uvicorn
        from .api import create_app
        uvicorn.run(create_app(_pipeline(args)), host=args.host, port=args.port)
        return 0

    pipeline = _pipeline(args)
    if args.cmd == "batch":
        result = pipeline.run_batch()
    elif args.cmd == "stream":
        result = pipeline.run_stream(max_batches=args.max_batches, interval_seconds=args.interval)
    elif args.cmd == "health":
        pipeline.run_batch()
        result = pipeline.health()
    else:  # pragma: no cover
        parser.error("unknown command")
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
