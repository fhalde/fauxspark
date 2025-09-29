import json
import os
import simpy
from colorama import init, Fore, Style
from typing import Mapping
from models import (
    RegisterExecutor,
    Stage,
    LaunchTask,
    KillTask,
    StatusUpdate,
    FetchFailed,
    KillExecutor,
    Task,
)


def main(DAG: list[Stage] = [], E=1, cores=1):
    env = simpy.Environment()

    def log(component, msg):
        nonlocal env
        print(f"{Style.BRIGHT}{Fore.RED}{env.now:6.2f}{Style.RESET_ALL}: [{component:<12}] {msg}")

    print("fauxspark!")
    scheduler_queue = simpy.Store(env)
    executors: Mapping[int, RegisterExecutor] = {}

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

    class FetchFailedException(Exception):
        def __init__(self, dep: int):
            self.dep = dep

    def executor(eid, executor_queue):
        running_tasks = {}

        def read_shuffle(stats, dep: int):
            try:
                yield env.timeout(stats["shuffle"]["avg"])
            except simpy.Interrupt as e:
                if e.cause == "fetchfailed":
                    raise FetchFailedException(dep)

        def thread(launch_task: LaunchTask):
            try:
                deps = DAG[launch_task.stage_id()].deps
                for dep in deps:
                    if DAG[dep].status != "completed":
                        yield executor_queue.put(FetchFailed(id=launch_task.id, dep=dep))
                        return
                    for task in DAG[dep].tasks:
                        executor_id = task.launched_tasks[task.current].executor_id
                        if executor_id not in executors:
                            yield executor_queue.put(FetchFailed(id=launch_task.id, dep=dep))
                            return
                        shuffle_process = env.process(
                            read_shuffle(DAG[launch_task.stage_id()].stats, dep)
                        )
                        executors[executor_id].running_shuffles[launch_task.id] = shuffle_process
                        yield shuffle_process
                yield env.timeout(DAG[launch_task.task.stage_id].stats["avg"])
                yield executor_queue.put(StatusUpdate(id=launch_task.id, status="completed"))
            except FetchFailedException as e:
                yield executor_queue.put(FetchFailed(id=launch_task.id, dep=e.dep))

        while True:
            msg = yield executor_queue.get()
            log(f"executor-{eid}", f"{msg!r}")
            match msg:
                case LaunchTask() as launch_task:
                    running_tasks[launch_task.id] = env.process(thread(launch_task))

                case StatusUpdate(id=id, status="completed") as status_update:
                    running_tasks.pop(id, None)
                    yield scheduler_queue.put(status_update)

                case FetchFailed(id=id) as fetch_failed:
                    running_tasks.pop(id, None)
                    yield scheduler_queue.put(fetch_failed)

                case KillTask() as kill_task:
                    process = running_tasks.pop(kill_task.id, None)
                    if process is None:
                        log(f"executor-{eid}", f"task={kill_task.id} not found")
                        continue
                    process.interrupt("killed")
                    yield scheduler_queue.put(StatusUpdate(kill_task, "killed"))
                case _:
                    log(f"executor-{eid}", f"unhandled: {msg!r}")

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
                    id=(id := nextid()),
                    executor_id=executor.id,
                    task=task,
                    status="running",
                )
                task.current = id
                task.launched_tasks[id], running_tasks[id], executor.running_tasks[id] = (
                    launch_task,
                    launch_task,
                    launch_task,
                )
                yield executor.queue.put(launch_task)
                executor.available_slots -= 1
            event = yield scheduler_queue.get()
            log("scheduler", f"{event!r}")
            match event:
                case RegisterExecutor(id=id) as executor:
                    executors[id] = executor

                case KillExecutor(id=id):
                    executor = executors[id]
                    for launched_task in executor.running_tasks.values():
                        tid = launched_task.id
                        log("scheduler", f"killing task {tid}")
                        task = launched_task.task
                        task.current = None
                        task.status, launched_task.status = "killed", "killed"
                        running_tasks.pop(tid)
                    for shuffle_process in executor.running_shuffles.values():
                        if shuffle_process.is_alive:
                            shuffle_process.interrupt("fetchfailed")
                    del executors[id]

                case FetchFailed(id=id, dep=dep):
                    if id in running_tasks:
                        launch_task = running_tasks.pop(id)
                        task = launch_task.task
                        current_stage = DAG[task.stage_id]
                        current_stage.status = "pending"
                        for task in current_stage.tasks:
                            task.status, task.current = "pending", None
                        parent_stage = DAG[dep]
                        parent_stage.status = "failed"
                        for task in parent_stage.tasks:
                            last_execution = task.launched_tasks[task.current]
                            if last_execution.executor_id not in executors:
                                task.status, task.current = "pending", None
                        executor = executors.get(launch_task.executor_id)
                        if executor:
                            executor.available_slots += 1
                            executor.running_tasks.pop(id, None)
                    else:
                        log("scheduler", f"{Fore.MAGENTA}stale {event!r}")

                case StatusUpdate(id=id, status="completed"):
                    launched_task = running_tasks.pop(id, None)
                    if launched_task is not None and launched_task.task.current == id:
                        task = launched_task.task
                        task.status, task.current = "completed", id
                        stage = DAG[task.stage_id]
                        if all(task.status == "completed" for task in stage.tasks):
                            stage.status = "completed"
                        executor = executors.get(launched_task.executor_id)
                        if executor:
                            executor.available_slots += 1
                            executor.running_tasks.pop(launched_task.id)
                    else:
                        log("scheduler", f"{Fore.MAGENTA}stale {event!r}")

                case _:
                    log("scheduler", f"unhandled: {event!r}")

    log("main", f"starting {E} executors...")

    def mk_executor(i):
        return RegisterExecutor(
            id=i,
            cores=cores,
            available_slots=cores,
            process=env.process(executor(i, (queue := simpy.Store(env)))),
            queue=queue,
            running_tasks={},
        )

    def start_executors():
        for i in range(E):
            yield scheduler_queue.put(mk_executor(i))

    log("main", "starting executors...")
    env.process(start_executors())

    log("main", "starting scheduler")
    env.process(scheduler())

    def killer():
        yield env.timeout(72)
        yield scheduler_queue.put(KillExecutor(id=0))
        yield scheduler_queue.put(mk_executor(1))

    env.process(killer())

    env.run()
    log("main", "simulation completed")


if __name__ == "__main__":
    init(autoreset=True)
    os.environ["PYTHONUNBUFFERED"] = "1"
    main(DAG=[Stage.model_validate(stage) for stage in json.load(open("dag.json"))], E=2, cores=1)
