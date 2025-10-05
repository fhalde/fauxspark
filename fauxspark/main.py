import argparse
import json
import os
import simpy
from colorama import init, Fore, Style
from .scheduler import Scheduler
from .executor import Executor
from .models import ExecutorKilled, Task
from . import util
from typing import Generator, Any
import sys
from pydantic import TypeAdapter
from .models import Stage
from . import dist


def main(DAG: list[Stage], args: argparse.Namespace) -> None:
    env = simpy.Environment()
    util.log(env, "main", "fauxspark!")
    scheduler = Scheduler(env, DAG)
    util.log(env, "main", f"starting {args.executors} executors...")

    def mk_executor(i: int) -> Executor:
        executor = Executor(
            env=env,
            DAG=DAG,
            executors=scheduler.executors,
            id=i,
            cores=args.cores,
            queue=simpy.Store(env),
            scheduler_queue=scheduler.scheduler_queue,
            scheduler=scheduler,
        )
        return executor

    def start_executors() -> None:
        for i in range(args.executors):
            executor = mk_executor(i)
            executor.start()
            scheduler.scheduler_queue.put(executor)

    util.log(env, "main", "starting executors...")
    start_executors()

    util.log(env, "main", "starting scheduler")
    scheduler.start()

    last_eid = args.executors

    def simulate_failure(eid: int, t: float) -> Generator[Any, None, None]:
        yield env.timeout(t)
        executor = scheduler.executors.get(eid, None)
        if executor is None:
            return
        executor.kill()
        scheduler.scheduler_queue.put(ExecutorKilled(eid=eid))
        if args.auto_replace:
            yield env.timeout(args.auto_replace_delay)
            nonlocal last_eid
            executor = mk_executor(last_eid)
            last_eid += 1
            executor.start()
            scheduler.scheduler_queue.put(executor)

    def simulate_auto_replace(t: float) -> Generator[Any, None, None]:
        yield env.timeout(t)
        nonlocal last_eid
        executor = mk_executor(last_eid)
        last_eid += 1
        executor.start()
        scheduler.scheduler_queue.put(executor)

    for eid, t in args.sf:
        env.process(simulate_failure(eid, t))

    for t in args.sa:
        env.process(simulate_auto_replace(t))

    env.run()
    if all(stage.status == "completed" for stage in scheduler.DAG):
        util.log(env, "main", f"{Fore.GREEN}job completed successfully")
    else:
        util.log(env, "main", f"{Fore.RED}job did not complete{Style.RESET_ALL}\n{DAG}")
        for stage in scheduler.DAG:
            util.log(env, "main", f"{stage.tasks!r}")


def cli() -> None:
    init(autoreset=True)
    os.environ["PYTHONUNBUFFERED"] = "1"

    parser = argparse.ArgumentParser(
        description="FauxSpark - A discrete event simulation modeling Apache Spark using SimPy"
    )
    parser.add_argument(
        "-e",
        "--executors",
        type=int,
        default=1,
        help="Set the number of executors to use (default: 1).",
    )
    parser.add_argument(
        "-c",
        "--cores",
        type=int,
        default=1,
        help="Specify how many cores each executor will have (default: 1).",
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Path to DAG JSON file",
    )

    def parse_sim_failure(text: str) -> tuple[int, float]:
        try:
            e, t = text.split(",")
            return (int(e), float(t))
        except ValueError:
            raise argparse.ArgumentTypeError("Each pair must look like (executor id, time)")

    def parse_sim_autoscale(text: str) -> float:
        try:
            return float(text)
        except ValueError:
            raise argparse.ArgumentTypeError("Each time must be a number")

    parser.add_argument(
        "--sf",
        nargs="+",
        default=[],
        type=parse_sim_failure,
        help="Specify list of failure events as pairs of (executor_id,time) to simulate executor failures.",
    )

    parser.add_argument(
        "--sa",
        nargs="+",
        default=[],
        type=parse_sim_autoscale,
        help="Specify times (t) at which autoscaling (adding a new executor) should take place.",
    )

    parser.add_argument(
        "-a",
        "--auto-replace",
        default=False,
        type=bool,
        help="Turn on/off auto-replacement of executors on failure.",
    )

    parser.add_argument(
        "-d",
        "--auto-replace-delay",
        default=1,
        type=int,
        help="Set the delay (in seconds) it takes to replace an executor on failure (default: 1).",
    )

    args = parser.parse_args()

    try:
        with open(args.file, "r") as f:
            dag = TypeAdapter(list[Stage]).validate_python(json.load(f))
            for stage in dag:
                if stage.input:
                    stage.input.weights = dist.weights(
                        stage.input.distribution, stage.input.partitions
                    )
                if stage.output:
                    stage.output.weights = dist.weights(
                        stage.output.distribution, stage.output.partitions
                    )
                stage.tasks = [
                    Task(index=i, status="pending", stage=stage) for i in range(stage.partitions)
                ]
    except FileNotFoundError:
        print(f"Error: DAG file '{args.file}' not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in DAG file '{args.file}': {e}")
        sys.exit(1)

    main(DAG=dag, args=args)


if __name__ == "__main__":
    cli()
