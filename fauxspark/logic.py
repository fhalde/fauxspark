from typing import Optional
from .executor import Executor


def next_available_executor(executors: dict[int, Executor]) -> Optional[Executor]:
    for executor in executors.values():
        if executor.cores_free > 0:
            return executor
    return None
