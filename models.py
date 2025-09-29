from pydantic import BaseModel
from typing import Any, List, Mapping
from colorama import Fore, Style


class Stage(BaseModel):
    id: int
    deps: List[int]
    status: str
    partitions: int
    stats: Mapping[str, float]
    tasks: List["Task"]

    def __repr__(self):
        return f"{Fore.CYAN}Stage{Style.RESET_ALL}(id={self.id}, status={self.status})"


class Task(BaseModel):
    index: int
    status: str
    stage_id: int
    current: set[int] = set()
    launched_tasks: Mapping[int, "LaunchTask"] = {}

    def __repr__(self):
        return f"{Fore.GREEN}Task{Style.RESET_ALL}(stage={self.stage_id}, index={self.index}, status={self.status})"


class LaunchTask(BaseModel):
    id: int
    executor_id: int
    task: Task
    status: str

    def stage_id(self):
        return self.task.stage_id

    def __repr__(self):
        return f"{Fore.YELLOW}LaunchTask{Style.RESET_ALL}(id={self.id}, executor_id={self.executor_id}, status={self.status}, task={self.task!r})"


class KillTask(BaseModel):
    id: int


class StatusUpdate(BaseModel):
    id: int
    status: str

    def __repr__(self):
        return f"{Fore.BLUE}StatusUpdate{Style.RESET_ALL}(id={self.id}, status={self.status})"


class FetchFailed(BaseModel):
    id: int
    launch_task: LaunchTask
    dep: int

    def __repr__(self):
        return f"{Fore.RED}FetchFailed{Style.RESET_ALL}(id={self.launch_task.id}, executor_id={self.launch_task.executor_id}, dep={self.dep})"


class Executor(BaseModel):
    id: int
    cores: int
    available_slots: int
    process: Any  # simpy process
    queue: Any
    running_tasks: Mapping[int, "LaunchTask"] = {}


class KillExecutor(BaseModel):
    id: int
