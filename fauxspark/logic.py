from typing import Mapping
from .models import Stage, Task
from .executor import Executor


def next_available_executor(executors: Mapping[int, Executor]):
    for executor in executors.values():
        if executor.available_slots > 0:
            return executor
    return None


def runnable_tasks(DAG: list[Stage]) -> list[tuple[Stage, Task]]:
    acc = []
    for stage in DAG:
        if stage.status != "completed" and all(
            DAG[dep].status == "completed" for dep in stage.deps
        ):
            for task in stage.tasks:
                if task.status not in ["completed", "running"]:
                    acc.append([stage, task])
    return acc
