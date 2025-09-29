from pydantic import BaseModel, ConfigDict, Field
from typing import Any, List, Mapping, Optional
from colorama import Fore, Style
import simpy


class Stage(BaseModel):
    id: int
    deps: List[int]
    status: str
    partitions: int
    stats: Mapping[Any, Any]
    tasks: List["Task"]

    def __repr__(self):
        return f"{Fore.CYAN}Stage{Style.RESET_ALL}(id={self.id}, status={self.status})"


class Task(BaseModel):
    index: int
    status: str
    stage_id: int
    current: Optional[int] = None
    launched_tasks: Mapping[int, "LaunchTask"] = Field(default_factory=dict)

    def __repr__(self):
        return f"{Fore.GREEN}Task{Style.RESET_ALL}(stage={self.stage_id}, index={self.index}, status={self.status})"


class LaunchTask(BaseModel):
    id: int
    executor_id: int
    task: "Task"
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
    executor_id: int

    def __repr__(self):
        return f"{Fore.BLUE}StatusUpdate{Style.RESET_ALL}(id={self.id}, status={self.status}, executor_id={self.executor_id})"


class FetchFailed(BaseModel):
    id: int
    dep: int
    executor_id: int

    def __repr__(self):
        return f"{Fore.RED}FetchFailed{Style.RESET_ALL}(id={self.id}, dep={self.dep}, executor_id={self.executor_id})"


class RegisterExecutor(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: int
    cores: int
    available_slots: int
    process: simpy.Process  # simpy process
    queue: simpy.Store  # simpy store
    running_tasks: Mapping[int, "LaunchTask"] = Field(default_factory=dict)
    running_shuffles: Mapping[int, simpy.Process] = Field(default_factory=dict)

    def __repr__(self):
        return f"{Fore.GREEN}Executor{Style.RESET_ALL}(id={self.id}, cores={self.cores}, available_slots={self.available_slots})"


class KillExecutor(BaseModel):
    id: int

    def __repr__(self):
        return f"{Fore.RED}KillExecutor{Style.RESET_ALL}(id={self.id})"
