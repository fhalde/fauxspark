import json
import os
import simpy
from colorama import init, Fore
from typing import Mapping
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

env = simpy.Environment()
log = partial(util.log, env)


def main(DAG: list[Stage] = [], E=1, cores=1):
    print("fauxspark!")
    scheduler_queue = simpy.Store(env)
    executors: Mapping[int, Executor] = {}
    nextid = util.nextid()

    def scheduler():
        scheduled: Mapping[int, LaunchTask] = {}

        def schedule_runnable_tasks():
            while (executor := next_available_executor(executors)) and (
                taskset := runnable_tasks(DAG)
            ) != []:
                stage, task = taskset.pop(0)
                stage.status, task.status = "running", "running"
                launch_task = LaunchTask(
                    tid=(id := next(nextid)),
                    eid=executor.id,
                    task=task,
                    status="running",
                )
                task.current = id
                task.launched_tasks[id], scheduled[id] = launch_task, launch_task
                util.put(executor.queue, launch_task)
                executor.reserve()

        def register_executor(executor: Executor):
            executors[executor.id] = executor

        def executor_killed(executor_killed: ExecutorKilled):
            executor = executors[executor_killed.eid]
            for tid in executor.taskprocs.keys():
                if launched_task := scheduled.pop(tid, None):
                    task = launched_task.task
                    task.status, task.current = "killed", None
                    launched_task.status = "killed"
            del executors[executor.id]

        def fetch_failed(fetch_failed: FetchFailed):
            launch_task = scheduled.pop(fetch_failed.tid, None)
            if launch_task:
                task = launch_task.task
                current_stage = DAG[task.stage_id]
                current_stage.status = "pending"
                for task in current_stage.tasks:
                    task.status, task.current = "pending", None
                parent_stage = DAG[fetch_failed.dep]
                parent_stage.status = "failed"
                for task in parent_stage.tasks:
                    if task.launched_tasks[task.current].eid not in executors:
                        task.status, task.current = "pending", None
                executor = executors.get(launch_task.eid, None)
                if executor:
                    executor.release()
            else:
                log("scheduler", f"{Fore.MAGENTA}stale {fetch_failed!r}")

        def status_update(status_update: StatusUpdate):
            launched_task = scheduled.pop(status_update.tid, None)
            if launched_task and launched_task.task.current == status_update.tid:
                task = launched_task.task
                match status_update.status:
                    case "completed":
                        task.status, task.current = "completed", status_update.tid
                        launched_task.status = "completed"
                        stage = DAG[task.stage_id]
                        if all(task.status == "completed" for task in stage.tasks):
                            stage.status = "completed"
                        executor = executors.get(launched_task.eid, None)
                    case "killed":
                        task.status, task.current = "killed", None
                        launched_task.status = "killed"
                        stage = DAG[task.stage_id]
                        executor = executors.get(launched_task.eid, None)
                if executor:
                    executor.release()
            else:
                log("scheduler", f"{Fore.MAGENTA}stale {status_update!r}")

        while True:
            schedule_runnable_tasks()
            event = yield scheduler_queue.get()
            log("scheduler", f"{event!r}")
            match event:
                case Executor():
                    register_executor(event)

                case FetchFailed():
                    fetch_failed(event)

                case ExecutorKilled():
                    executor_killed(event)

                case StatusUpdate():
                    status_update(event)

                case _:
                    log("scheduler", f"unhandled: {event!r}")

    log("main", f"starting {E} executors...")

    def mk_executor(i):
        executor = Executor(
            env=env,
            DAG=DAG,
            executors=executors,
            id=i,
            cores=cores,
            queue=simpy.Store(env),
            scheduler_queue=scheduler_queue,
        )
        return executor

    def start_executors():
        for i in range(E):
            executor = mk_executor(i)
            executor.start()
            scheduler_queue.put(executor)

    log("main", "starting executors...")
    start_executors()

    log("main", "starting scheduler")
    env.process(scheduler())

    def simulate_a_failure():
        # just before the last task is about to finish
        yield env.timeout(24)
        executor = executors.get(0)
        executor.kill()
        scheduler_queue.put(ExecutorKilled(eid=0))
        executor = mk_executor(1)
        executor.start()
        scheduler_queue.put(executor)

    env.process(simulate_a_failure())

    env.run()
    log("main", "simulation completed")


def cli():
    init(autoreset=True)
    os.environ["PYTHONUNBUFFERED"] = "1"
    main(
        DAG=[
            Stage.model_validate(stage)
            for stage in json.load(open(os.path.join(os.path.dirname(__file__), "dag.json")))
        ],
        E=1,
        cores=1,
    )


if __name__ == "__main__":
    cli()
