from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Mapping, Optional
from colorama import Fore, Style
import numpy as np


class Input(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    size: int
    partitions: int
    distribution: dict[Any, Any]
    weights: np.ndarray


class Output(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    ratio: float
    partitions: int
    distribution: dict[Any, Any]
    weights: np.ndarray


class Stage(BaseModel):
    id: int
    deps: List[int]
    status: str
    partitions: int
    stats: Mapping[Any, Any]
    tasks: List["Task"]
    input: Optional[Input] = None
    output: Optional[Output] = None

    def __repr__(self: "Stage") -> str:
        return f"{Fore.CYAN}Stage{Style.RESET_ALL}(id={self.id}, status={self.status}, deps={self.deps})"


class Task(BaseModel):
    index: int
    status: str
    stage: "Stage"
    current: Optional[int] = None
    launched_tasks: dict[int, "LaunchTask"] = Field(default_factory=dict)

    def __repr__(self: "Task") -> str:
        return f"{Fore.GREEN}Task{Style.RESET_ALL}(stage={self.stage.id}, index={self.index}, status={self.status})"


class LaunchTask(BaseModel):
    tid: int
    eid: int
    task: "Task"
    status: str

    def __repr__(self: "LaunchTask") -> str:
        return f"{Fore.YELLOW}LaunchTask{Style.RESET_ALL}(id={self.tid}, executor_id={self.eid}, status={self.status}, task={self.task!r})"


class KillTask(BaseModel):
    tid: int


class StatusUpdate(BaseModel):
    tid: int
    status: str
    eid: int

    def __repr__(self: "StatusUpdate") -> str:
        return f"{Fore.BLUE}StatusUpdate{Style.RESET_ALL}(id={self.tid}, status={self.status}, executor_id={self.eid})"


class FetchFailed(BaseModel):
    tid: int
    dep: int
    eid: int

    def __repr__(self: "FetchFailed") -> str:
        return f"{Fore.RED}FetchFailed{Style.RESET_ALL}(id={self.tid}, dep={self.dep}, executor_id={self.eid})"


class ExecutorKilled(BaseModel):
    eid: int

    def __repr__(self: "ExecutorKilled") -> str:
        return f"{Fore.RED}ExecutorKilled{Style.RESET_ALL}(id={self.eid})"
