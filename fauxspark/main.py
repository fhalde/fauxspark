import argparse
import json
import os
from typing import Generator
import typing
import simpy
from colorama import Style, init, Fore
from fauxspark.executor import Executor
from .models import (
    Stage,
    LaunchTask,
    StatusUpdate,
    FetchFailed,
    ExecutorKilled,
)
from .logic import next_available_executor, runnable_tasks
from . import util
from functools import partial
import sys


class Scheduler(object):
    def __init__(self, env: simpy.Environment, DAG: list[Stage]):
        self.env = env
        self.DAG = DAG
        self.executors: dict[int, Executor] = dict()
        # tuple of dep (stage id) and partition (task index)
        self.shuffles: dict[(int, int), Executor] = dict()  # type: ignore
        self.scheduled: dict[int, LaunchTask] = dict()
        self.scheduler_queue = simpy.Store(env)
        self.nextid: Generator[int, None, None] = util.nextidgen()
        self.logger = partial(util.log, env, "scheduler")

    def start(self: "Scheduler") -> simpy.Process:
        return self.env.process(self.loop())

    def loop(self: "Scheduler") -> Generator[typing.Any, None, None]:
        while True:
            self.schedule_runnable_tasks()
            event = yield self.scheduler_queue.get()
            self.logger(f"{event!r}")
            match event:
                case Executor():
                    self.register_executor(event)

                case FetchFailed():
                    self.fetch_failed(event)

                case ExecutorKilled():
                    self.executor_killed(event)

                case StatusUpdate():
                    self.status_update(event)

                case _:
                    self.logger(f"unhandled: {event!r}")

    def schedule_runnable_tasks(self: "Scheduler") -> None:
        while (executor := next_available_executor(self.executors)) and (
            taskset := runnable_tasks(self.DAG)
        ) != []:
            stage, task = taskset.pop(0)
            stage.status, task.status = "running", "running"
            launch_task = LaunchTask(
                tid=(id := next(self.nextid)),
                eid=executor.id,
                task=task,
                status="running",
            )
            task.current = id
            task.launched_tasks[id], self.scheduled[id] = launch_task, launch_task
            util.put(executor.queue, launch_task)
            executor.reserve()

    def register_executor(self: "Scheduler", executor: Executor) -> None:
        self.executors[executor.id] = executor

    def executor_killed(self: "Scheduler", executor_killed: ExecutorKilled) -> None:
        executor = self.executors[executor_killed.eid]
        for tid in executor.taskprocs.keys():
            if launched_task := self.scheduled.pop(tid, None):
                task = launched_task.task
                task.status, task.current = "killed", None
                launched_task.status = "killed"
        del self.executors[executor.id]

    def fetch_failed(self: "Scheduler", fetch_failed: FetchFailed) -> None:
        launch_task = self.scheduled.pop(fetch_failed.tid, None)
        if launch_task:
            task = launch_task.task
            current_stage = self.DAG[task.stage_id]
            current_stage.status = "pending"
            for task in current_stage.tasks:
                task.status, task.current = "pending", None
            parent_stage = self.DAG[fetch_failed.dep]
            parent_stage.status = "failed"
            for task in parent_stage.tasks:
                if task.launched_tasks[task.current].eid not in self.executors:  # type: ignore
                    task.status, task.current = "pending", None
            executor = self.executors.get(launch_task.eid, None)
            if executor:
                executor.release()
        else:
            self.logger(f"{Fore.MAGENTA}stale {fetch_failed!r}")

    def status_update(self: "Scheduler", status_update: StatusUpdate) -> None:
        launched_task = self.scheduled.pop(status_update.tid, None)
        if launched_task and launched_task.task.current == status_update.tid:
            task = launched_task.task
            match status_update.status:
                case "completed":
                    task.status, task.current = "completed", status_update.tid
                    launched_task.status = "completed"
                    stage = self.DAG[task.stage_id]
                    if all(task.status == "completed" for task in stage.tasks):
                        stage.status = "completed"
                    executor = self.executors.get(launched_task.eid, None)
                case "killed":
                    task.status, task.current = "killed", None
                    launched_task.status = "killed"
                    stage = self.DAG[task.stage_id]
                    executor = self.executors.get(launched_task.eid, None)
            if executor:
                executor.release()
        else:
            self.logger(f"{Fore.MAGENTA}stale {status_update!r}")


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

    def simulate_a_failure() -> Generator[typing.Any, None, None]:
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
