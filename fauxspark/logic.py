from typing import Optional
from .executor import Executor
from .models import Stage, Task


def next_available_executor(executors: dict[int, Executor]) -> Optional[Executor]:
    for executor in executors.values():
        if executor.cores_free > 0:
            return executor
    return None


def runnable_tasks(stages: list[Stage]) -> list[tuple[Stage, Task]]:
    stage_by_id: dict[int, Stage] = {stage.id: stage for stage in stages}
    runnable: list[tuple[Stage, Task]] = []
    for stage in stages:
        if not all(stage_by_id[dep].status == "completed" for dep in stage.deps):
            continue
        for task in stage.tasks:
            if task.status == "pending":
                runnable.append((stage, task))
    return runnable
