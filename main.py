from typing import Mapping
import simpy
import os
import json
from models import (
    Executor,
    Stage,
    LaunchTask,
    KillTask,
    StatusUpdate,
    FetchFailed,
    KillExecutor,
    Task,
)

from colorama import init, Fore, Style

init(autoreset=True)


def main(DAG: list[Stage] = [], E=1, cores=1):
    env = simpy.Environment()

    def log(msg):
        nonlocal env

        print(f"{Style.BRIGHT}{Fore.RED}{env.now:6.2f}{Style.RESET_ALL}: {msg}")

    print("fauxspark!")
    scheduler_queue = simpy.Store(env)
    executors: Mapping[int, Executor] = {}

    def next_available_executor():
        for executor in executors.values():
            if executor.available_slots > 0:
                return executor
        return None

    def schedulable_tasks() -> list[tuple[Stage, Task]]:
        acc = []
        for stage in DAG:
            if stage.status != "completed" and all(
                DAG[dep].status == "completed" for dep in stage.deps
            ):
                for task in stage.tasks:
                    if task.status not in ["completed", "running"]:
                        acc.append([stage, task])
        return acc

    def executor(id, executor_queue):
        running_tasks = {}

        def thread(launch_task: LaunchTask):
            try:
                yield env.timeout(DAG[launch_task.task.stage_id].stats["avg"])
                executor_queue.put(StatusUpdate(id=launch_task.id, status="completed"))
            except simpy.Interrupt:
                log(f"executor {id} interrupted task={launch_task!r}")
                return

        while True:
            msg = yield executor_queue.get()
            match msg:
                case LaunchTask() as launch_task:
                    log(f"executor={id} {launch_task!r}")
                    running_tasks[launch_task.id] = env.process(thread(launch_task))

                case StatusUpdate(id=id, status="completed") as status_update:
                    running_tasks.pop(id)
                    scheduler_queue.put(status_update)

                case KillTask() as kill_task:
                    log(f"executor={id} kill task={kill_task.id}")
                    process = running_tasks.pop(kill_task.id, None)
                    if process is None:
                        log(f"executor={id} task={kill_task.id} not found")
                        continue
                    process.interrupt({"cause": "killed"})
                    # scheduler_queue.put(StatusUpdate(kill_task, "killed"))
                case _:
                    log(f"executor={id} unknown message={msg!r}")

    def scheduler():
        taskid = 0
        running_tasks: Mapping[int, LaunchTask] = {}

        def nextid():
            nonlocal taskid
            taskid += 1
            return taskid

        while True:
            while (executor := next_available_executor()) and (
                runnable_tasks := schedulable_tasks()
            ) != []:
                stage, task = runnable_tasks.pop(0)
                stage.status, task.status = "running", "running"
                launch_task = LaunchTask(
                    id=nextid(),
                    executor_id=executor.id,
                    task=task,
                    status="running",
                )
                running_tasks[launch_task.id] = launch_task
                executor.running_tasks[launch_task.id] = launch_task
                yield executor.queue.put(launch_task)
                executor.available_slots -= 1
            event = yield scheduler_queue.get()
            match event:
                case Executor(id=id) as executor:
                    log(f"register executor {id}")
                    executors[id] = executor

                case KillExecutor(id=id):
                    log(f"kill executor {id}")
                    executor = executors[id]
                    for launched_task in executor.running_tasks.values():
                        task = launched_task.task
                        task.status = "killed"
                        launched_task.status = "killed"
                        running_tasks.pop(launched_task.id)
                    del executors[id]

                case FetchFailed(id=id, dep=dep):
                    if id in running_tasks:
                        launch_task = running_tasks.pop(id)
                        task = launch_task.task
                        stage = DAG[task.stage_id]
                        stage.status = "pending"
                        for task in stage.tasks:
                            task.status = "pending"
                            task.current = set()
                            task.launched_tasks = {}
                        for dep in stage.deps:
                            DAG[dep].status = "failed"
                            for task in DAG[dep].tasks:
                                task.status = "pending"
                                task.current = set()
                                task.launched_tasks = {}

                case StatusUpdate(id=id, status="completed"):
                    if id in running_tasks:
                        log(f"status update task={id} completed")
                        launched_task: LaunchTask = running_tasks.pop(id)
                        task: Task = launched_task.task
                        stage: Stage = DAG[task.stage_id]
                        task.status = "completed"
                        task.current.add(id)
                        if all(task.status == "completed" for task in stage.tasks):
                            stage.status = "completed"
                        executor = executors[launched_task.executor_id]
                        executor.available_slots += 1
                        executor.running_tasks.pop(launched_task.id)
                    else:
                        log(f"status update {id} completed but not in running tasks")

    log(f"starting {E} executors")
    for i in range(E):
        scheduler_queue.put(
            Executor(
                id=i,
                cores=cores,
                available_slots=cores,
                process=env.process(executor(i, (queue := simpy.Store(env)))),
                queue=queue,
                running_tasks={},
            )
        )

    log("starting scheduler")
    env.process(scheduler())

    env.run()
    log("simulation completed")


if __name__ == "__main__":
    os.environ["PYTHONUNBUFFERED"] = "1"
    main(DAG=[Stage.model_validate(stage) for stage in json.load(open("dag.json"))], E=10, cores=10)
