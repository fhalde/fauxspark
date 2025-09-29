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
            tid = launch_task.tid
            try:
                deps = DAG[launch_task.stage_id()].deps
                for dep in deps:
                    if DAG[dep].status != "completed":
                        yield executor_queue.put(FetchFailed(tid=tid, dep=dep, eid=eid))
                        return
                    for task in DAG[dep].tasks:
                        target = task.launched_tasks[task.current].eid
                        if target not in executors:
                            yield executor_queue.put(FetchFailed(tid=tid, dep=dep, eid=eid))
                            return
                        shuffle_process = env.process(
                            read_shuffle(DAG[launch_task.stage_id()].stats, dep)
                        )
                        executors[eid].running_shuffles[tid] = shuffle_process
                        yield shuffle_process
                yield env.timeout(DAG[launch_task.stage_id()].stats["avg"])
                yield executor_queue.put(StatusUpdate(tid=tid, status="completed", eid=eid))
            except FetchFailedException as e:
                yield executor_queue.put(FetchFailed(tid=tid, dep=e.dep, eid=eid))

        while True:
            event = yield executor_queue.get()
            log(f"executor-{eid}", f"{event!r}")
            match event:
                case LaunchTask(tid=tid):
                    running_tasks[tid] = env.process(thread(event))

                case StatusUpdate(tid=tid, status="completed"):
                    running_tasks.pop(tid, None)
                    yield scheduler_queue.put(event)

                case FetchFailed(tid=tid):
                    running_tasks.pop(tid, None)
                    yield scheduler_queue.put(event)

                case KillTask(tid=tid):
                    process = running_tasks.pop(tid, None)
                    if process is None:
                        log(f"executor-{eid}", f"task={tid} not found")
                        continue
                    process.interrupt("killed")
                    yield scheduler_queue.put(StatusUpdate(tid=tid, status="killed", eid=eid))
                case _:
                    log(f"executor-{eid}", f"unhandled: {event!r}")

    def scheduler():
        taskid = 0
        running_tasks: Mapping[int, LaunchTask] = {}

        def nextid():
            nonlocal taskid
            taskid += 1
            return taskid

        def schedule_tasks():
            while (executor := next_available_executor()) and (
                runnable_tasks := schedulable_tasks()
            ) != []:
                stage, task = runnable_tasks.pop(0)
                stage.status, task.status = "running", "running"
                launch_task = LaunchTask(
                    tid=(id := nextid()),
                    eid=executor.id,
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

        def register_executor(register_executor: RegisterExecutor):
            executors[register_executor.id] = register_executor

        def kill_executor(kill_executor: KillExecutor):
            executor = executors[kill_executor.eid]
            for launched_task in executor.running_tasks.values():
                log("scheduler", f"killing task {launched_task.tid}")
                task = launched_task.task
                task.current, task.status = None, "killed"
                launched_task.status = "killed"
                running_tasks.pop(launched_task.tid)
            for shuffle_process in executor.running_shuffles.values():
                if shuffle_process.is_alive:
                    shuffle_process.interrupt("fetchfailed")
            del executors[executor.id]

        def fetch_failed(fetch_failed: FetchFailed):
            tid = fetch_failed.tid
            if tid in running_tasks:
                launch_task = running_tasks.pop(tid)
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
                executor = executors.get(launch_task.eid)
                if executor:
                    executor.available_slots += 1
                    executor.running_tasks.pop(tid, None)
            else:
                log("scheduler", f"{Fore.MAGENTA}stale {fetch_failed!r}")

        def status_update(status_update: StatusUpdate):
            tid = status_update.tid
            launched_task = running_tasks.pop(tid, None)
            if launched_task and launched_task.task.current == tid:
                task = launched_task.task
                task.status, task.current = "completed", tid
                stage = DAG[task.stage_id]
                if all(task.status == "completed" for task in stage.tasks):
                    stage.status = "completed"
                executor = executors.get(launched_task.eid)
                if executor:
                    executor.available_slots += 1
                    executor.running_tasks.pop(tid)
            else:
                log("scheduler", f"{Fore.MAGENTA}stale {status_update!r}")

        while True:
            yield from schedule_tasks()
            event = yield scheduler_queue.get()
            log("scheduler", f"{event!r}")
            match event:
                case RegisterExecutor():
                    register_executor(event)

                case KillExecutor():
                    kill_executor(event)

                case FetchFailed():
                    fetch_failed(event)

                case StatusUpdate(status="completed"):
                    status_update(event)

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
        yield scheduler_queue.put(KillExecutor(eid=0))
        yield scheduler_queue.put(mk_executor(1))

    env.process(killer())

    env.run()
    log("main", "simulation completed")


if __name__ == "__main__":
    init(autoreset=True)
    os.environ["PYTHONUNBUFFERED"] = "1"
    main(DAG=[Stage.model_validate(stage) for stage in json.load(open("dag.json"))], E=1, cores=1)
