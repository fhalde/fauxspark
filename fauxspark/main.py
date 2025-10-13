import argparse
import random
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
import numpy as np

util.LOG = False


def main(args: dict[str, Any], seed: int) -> None:
    np.random.seed(seed)
    try:
        with open(args["file"], "r") as f:
            DAG = util.init_dag(json.load(f))
    except FileNotFoundError:
        print(f"Error: DAG file {args['file']} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in DAG file {args['file']}: {e}")
        sys.exit(1)
    env = simpy.Environment()
    util.log(env, "main", f"random seed: {seed}")
    util.log(env, "main", "fauxspark!")
    scheduler = Scheduler(env, DAG)
    util.log(env, "main", f"starting {args['executors']} executors...")

    def mk_executor(i: int) -> Executor:
        executor = Executor(
            env=env,
            DAG=DAG,
            id=i,
            cores=args["cores"],
            queue=simpy.Store(env),
            scheduler_queue=scheduler.scheduler_queue,
            scheduler=scheduler,
        )
        return executor

    def start_executors() -> None:
        for i in range(args["executors"]):
            executor = mk_executor(i)
            executor.start()
            scheduler.scheduler_queue.put(executor)

    util.log(env, "main", "starting executors...")
    start_executors()

    util.log(env, "main", "starting scheduler")
    scheduler.start()

    last_eid = args["executors"]

    def simulate_failure(eid: int, t: float) -> Generator[Any, None, None]:
        yield env.timeout(t)
        executor = scheduler.executors.get(eid, None)
        if executor is None:
            return
        executor.kill()
        scheduler.scheduler_queue.put(ExecutorKilled(eid=eid))
        if args.get("auto_replace", False):
            yield env.timeout(args["auto_replace_delay"])
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

    for eid, t in args.get("sf", []):
        env.process(simulate_failure(eid, t))

    for t in args.get("sa", []):
        env.process(simulate_auto_replace(t))

    env.run()
    # stats
    stats = {}
    computed = sum([executor.computed for executor in scheduler.executors.values()])
    total = sum(
        [
            ((executor.end_time or env.now) - executor.start_time) * executor.cores
            for executor in scheduler.executors.values()
        ]
    )
    eff = computed / total
    stats["utilization"] = eff
    stats["runtime"] = env.now
    util.log(env, "main", f"{Fore.YELLOW}utilization: {eff}")
    if all(stage.status == "completed" for stage in scheduler.DAG):
        util.log(env, "main", f"{Fore.GREEN}job completed successfully")
        util.log(env, "report", f"{Fore.WHITE}{json.dumps(stats)}")
    else:
        util.log(env, "main", f"{Fore.RED}job did not complete{Style.RESET_ALL}\n{DAG}")
        for stage in scheduler.DAG:
            util.log(env, "main", f"{stage.tasks!r}")
    return stats


def cli() -> None:
    np.set_printoptions(precision=4, suppress=True)
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

    parser.add_argument(
        "--seed",
        default=None,
        type=int,
        help="Set the seed for the random number generator.",
    )

    args = parser.parse_args()
    seed = args.seed or random.randint(0, 1000000)
    main(args=vars(args), seed=seed)


if __name__ == "__main__":
    cli()


def optimizer(waste: float, runtime: float) -> None:
    """
    Find the optimal number of cores to use to keep p90 runtime & waste below the desired thresholds.
    """
    import random as rand
    import numpy as np

    init = random.randint(0, 1000000)
    util.LOG = False

    for cores in range(1, 11):
        stats = []
        rand.seed(init)  # reset seed for fairness
        for _ in range(10000):  # 1000 sims per cores configuration
            stat = main(
                args={"executors": 1, "cores": cores, "file": "./examples/simple/dag.json"},
                seed=rand.randint(0, 1000000),
            )
            stats.append(stat)
        wastes = list(map(lambda x: 1 - x["utilization"], stats))
        runtimes = list(map(lambda x: x["runtime"], stats))
        w = np.percentile(wastes, 90)
        r = np.percentile(runtimes, 90)
        if r < runtime and w < waste:
            print(
                f"{Fore.GREEN}✅ candidate configuration: cores={cores} has given p90 waste {w} and p90 runtime {r}{Style.RESET_ALL}"
            )
        else:
            print(
                f"{Fore.RED}👎 candidate configuration: cores={cores} has given p90 waste {w} and p90 runtime {r}{Style.RESET_ALL}"
            )
