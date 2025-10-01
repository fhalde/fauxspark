import argparse
import json
import os
import simpy
from colorama import init, Fore, Style
from .scheduler import Scheduler
from .executor import Executor
from .models import ExecutorKilled
from . import util
from typing import Generator, Any
import sys

from .models import Stage


def main(DAG: list[Stage] = [], E: int = 1, cores: int = 1) -> None:
    env = simpy.Environment()
    util.log(env, "main", "fauxspark!")
    scheduler = Scheduler(env, DAG)
    util.log(env, "main", f"starting {E} executors...")

    def mk_executor(i: int) -> Executor:
        executor = Executor(
            env=env,
            DAG=DAG,
            executors=scheduler.executors,
            id=i,
            cores=cores,
            queue=simpy.Store(env),
            scheduler_queue=scheduler.scheduler_queue,
            scheduler=scheduler,
        )
        return executor

    def start_executors() -> None:
        for i in range(E):
            executor = mk_executor(i)
            executor.start()
            scheduler.scheduler_queue.put(executor)

    util.log(env, "main", "starting executors...")
    start_executors()

    util.log(env, "main", "starting scheduler")
    scheduler.start()

    def simulate_a_failure() -> Generator[Any, None, None]:
        # just before the last task is about to finish
        yield env.timeout(24)
        executor = scheduler.executors.get(0)
        executor.kill()  # type: ignore
        scheduler.scheduler_queue.put(ExecutorKilled(eid=0))
        executor = mk_executor(1)
        executor.start()
        scheduler.scheduler_queue.put(executor)

    env.process(simulate_a_failure())

    env.run()
    if all(stage.status == "completed" for stage in scheduler.DAG):
        util.log(env, "main", f"{Fore.GREEN}job completed successfully")
    else:
        util.log(env, "main", f"{Fore.RED}job did not complete{Style.RESET_ALL}\n${DAG}")


def cli() -> None:
    init(autoreset=True)
    os.environ["PYTHONUNBUFFERED"] = "1"

    parser = argparse.ArgumentParser(description="FauxSpark - A Spark simulation framework")
    parser.add_argument(
        "-e", "--executors", type=int, default=1, help="Number of executors (default: 1)"
    )
    parser.add_argument(
        "-c", "--cores", type=int, default=1, help="Number of cores per executor (default: 1)"
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Path to DAG JSON file",
    )

    args = parser.parse_args()

    try:
        with open(args.file, "r") as f:
            dag = json.load(f)
    except FileNotFoundError:
        print(f"Error: DAG file '{args.file}' not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in DAG file '{args.file}': {e}")
        sys.exit(1)

    main(
        DAG=[Stage.model_validate(stage) for stage in dag],
        E=args.executors,
        cores=args.cores,
    )


if __name__ == "__main__":
    cli()
