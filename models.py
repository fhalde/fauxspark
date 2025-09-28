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
    current: set[int] = set()
    instances: Mapping[int, "TaskInstance"] = {}

    def __repr__(self):
        return f"Task(index={self.index}, status={self.status}, stage_id={self.stage_id}, instances={len(self.instances)})"


class TaskInstance(BaseModel):
    id: int
    executor_id: int
    task: Task
    status: str

    def stage_id(self):
        return self.task.stage_id

    def __repr__(self):
        return f"TaskInstance(id={self.id}, executor_id={self.executor_id}, task={self.task}, status={self.status})"


class LaunchTask(BaseModel):
    task_inst: TaskInstance

    def task_id(self):
        return self.task_inst.id

    def stage_id(self):
        return self.task_inst.task.stage_id

    def __repr__(self):
        return f"LaunchTask(id={self.task_inst.id}, index={self.task_inst.task.index}, stage_id={self.task_inst.task.stage_id})"


class KillTask(BaseModel):
    id: int


class StatusUpdate(BaseModel):
    id: int
    task_inst: TaskInstance
    status: str

    def task_id(self):
        return self.task_inst.id

    def __repr__(self):
        return f"StatusUpdate(id={self.task_inst.id}, index={self.task_inst.task.index}, stage_id={self.task_inst.task.stage_id}, status={self.status})"


class FetchFailed(BaseModel):
    launch_task_ref: LaunchTask
    dep: int

    def __repr__(self):
        return f"FetchFailed(id={self.launch_task_ref.task_inst.id}, index={self.launch_task_ref.task_inst.task.index}, stage_id={self.launch_task_ref.task_inst.task.stage_id}, dep={self.dep})"


class Executor(BaseModel):
    id: int
    cores: int
    available_slots: int
    process: Any  # simpy process
    queue: Any
    running_tasks: Mapping[int, "TaskInstance"] = {}


class KillExecutor(BaseModel):
    id: int
