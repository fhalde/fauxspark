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
    TaskInstance,
)


def main(DAG: list[Stage] = [], E=1, cores=1):
    env = simpy.Environment()

    def log(msg):
        nonlocal env
        print(f"{env.now}: {msg}")

    log("fauxspark!")
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

        def thread(task_inst: TaskInstance):
            try:
                yield env.timeout(DAG[task_inst.task.stage_id].stats["avg"])
                executor_queue.put(
                    StatusUpdate(id=task_inst.id, task_inst=task_inst, status="completed")
                )
            except simpy.Interrupt:
                log(f"executor {id} interrupted taskref={task_inst}")
                return

        while True:
            msg = yield executor_queue.get()
            match msg:
                case LaunchTask(task_inst=task_inst):
                    log(f"executor={id} task_inst={task_inst}")
                    running_tasks[task_inst.id] = env.process(thread(task_inst))

                case StatusUpdate(id=id, task_inst=task_inst):
                    running_tasks.pop(id)
                    scheduler_queue.put(
                        StatusUpdate(id=id, task_inst=task_inst, status="completed")
                    )

                case KillTask(id=task_id):
                    log(f"executor={id} kill task={task_id}")
                    process = running_tasks.pop(task_id, None)
                    if process is None:
                        log(f"executor={id} task={task_id} not found")
                        continue
                    process.interrupt({"cause": "killed"})
                    # scheduler_queue.put(StatusUpdate(kill_task, "killed"))
                case _:
                    log(f"executor={id} unknown message={msg}")

    def scheduler():
        taskid = 0
        running_tasks = set()

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
                task_instance = TaskInstance(
                    id=nextid(),
                    executor_id=executor.id,
                    task=task,
                    status="running",
                )
                running_tasks.add(task_instance.id)
                executor.running_tasks[task_instance.id] = task_instance
                print(f"launching task {task_instance.id}")
                yield executor.queue.put(LaunchTask(task_inst=task_instance))
                executor.available_slots -= 1
            print("scheduler waiting")
            event = yield scheduler_queue.get()
            match event:
                case Executor(id=id) as executor:
                    print(f"{env.now}: register executor {id}")
                    executors[id] = executor
                case KillExecutor(id=id):
                    print(f"{env.now}: kill executor {id}")
                    executor = executors[id]
                    for taskinst in executor.running_tasks.values():
                        DAG[taskinst.task.stage_id].tasks[taskinst.task.index].status = "killed"
                        running_tasks.remove(taskinst.id)
                    del executors[id]
                case FetchFailed(LaunchTask(task_inst=task_inst), dep):
                    stage = DAG[task_inst.task.stage_id]
                    # always reset the current stage.
                    stage.status = "pending"
                    for task in stage.tasks:
                        task.status = "pending"
                        # send KillTask message to all instances?
                        task.instances = {}
                    # mark deps as failed (all or some?)
                    for dep in stage.deps:
                        DAG[dep].status = "failed"
                        for task in DAG[dep].tasks:
                            # not all task need to be reset?
                            task.status = "pending"
                    print(f"{env.now}: fetch failed for task={task_inst.id} for dep={dep}")
                case StatusUpdate(id=id, task_inst=task_inst, status="completed"):
                    print(f"{env.now}: status update {task_inst.id} completed")
                    if id not in running_tasks:
                        log(f"status update {id} completed but not in running tasks")
                        continue
                    # update task status
                    stage = DAG[task_inst.task.stage_id]
                    task_inst.task.status = "completed"
                    task_inst.task.current.add(id)
                    # update stage status
                    if all(task.status == "completed" for task in stage.tasks):
                        stage.status = "completed"
                    executor = executors[task_inst.executor_id]
                    executor.available_slots += 1
                    executor.running_tasks.pop(task_inst.id)
                    running_tasks.remove(task_inst.id)

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

    start_time = env.now
    env.run()
    end_time = env.now
    print(f"Total time taken: {end_time - start_time}")


if __name__ == "__main__":
    os.environ["PYTHONUNBUFFERED"] = "1"
    main(DAG=[Stage.model_validate(stage) for stage in json.load(open("dag.json"))])
