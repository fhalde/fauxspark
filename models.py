from pydantic import BaseModel
from typing import Any, List, Mapping


class Stage(BaseModel):
    id: int
    deps: List[int]
    status: str
    partitions: int
    stats: Mapping[str, float]
    tasks: List["Task"]

    def __repr__(self):
        return f"Stage(id={self.id}, status={self.status})"


class Task(BaseModel):
    index: int
    status: str
    stage_id: int
    instances: Mapping[int, "TaskInstance"] = {}

    def __repr__(self):
        return f"Task(index={self.index}, status={self.status}, stage_id={self.stage_id}, instances={len(self.instances)})"


class TaskInstance(BaseModel):
    id: int
    executor_id: int
    task: Task
    status: str

    def __repr__(self):
        return f"TaskInstance(id={self.id}, executor_id={self.executor_id}, task={self.task}, status={self.status})"


class LaunchTask(BaseModel):
    task: Task

    def __repr__(self):
        return f"LaunchTask(id={self.task['id']}, index={self.task['index']}, stage_id={self.task['stage_id']})"


class KillTask(BaseModel):
    id: int


class StatusUpdate(BaseModel):
    launch_task_ref: LaunchTask
    status: str

    def __repr__(self):
        return f"StatusUpdate(id={self.launch_task_ref.task['id']}, index={self.launch_task_ref.task['index']}, stage_id={self.launch_task_ref.task['stage_id']}, status={self.status})"


class FetchFailed(BaseModel):
    launch_task_ref: LaunchTask
    dep: int

    def __repr__(self):
        return f"FetchFailed(id={self.launch_task_ref.task['id']}, index={self.launch_task_ref.task['index']}, stage_id={self.launch_task_ref.task['stage_id']}, dep={self.dep})"


class RegisterExecutor(BaseModel):
    id: int
    cores: int
    available_slots: int
    instance: Any  # simpy process
    queue: Any
    running_tasks: Mapping[int, "Task"] = {}


class KillExecutor(BaseModel):
    id: int


class KillTask(BaseModel):
    id: int
