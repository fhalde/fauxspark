from typing import Optional
from .models import Stage, Task
from .executor import Executor
import time


def next_available_executor(executors: dict[int, Executor]) -> Optional[Executor]:
    for executor in executors.values():
        if executor.cores_free > 0:
            return executor
    return None


def runnable_tasks(DAG: list[Stage]) -> list[tuple[Stage, Task]]:
    acc: list[tuple[Stage, Task]] = []
    for stage in DAG:
        if stage.status != "completed" and all(
            DAG[dep].status == "completed" for dep in stage.deps
        ):
            for task in stage.tasks:
                if task.status not in ["completed", "running"]:
                    acc.append(
                        (
                            stage,
                            task,
                        )
                    )
    return acc
