from typing import Any, Generator
from colorama import Style, Fore
from pydantic import TypeAdapter
import simpy
import numpy as np
from fauxspark import dist
from fauxspark.models import Stage, Task


def log(env: simpy.Environment, component: str, msg: str) -> None:
    print(f"{Style.BRIGHT}{Fore.RED}{env.now:6.2f}{Style.RESET_ALL}: [{component:<12}] {msg} ")


def nextidgen() -> Generator[int, None, None]:
    taskid = 0
    while True:
        yield taskid
        taskid += 1


def put(q: simpy.Store, event: Any) -> None:
    q.put(event)


def init_dag(m) -> list[Stage]:
    """
    m: topologically sorted list of stages
    """
    dag = TypeAdapter(list[Stage]).validate_python(m)
    for stage in dag:
        if stage.input:
            stage.input.splits = (
                dist.weights(stage.input.distribution, stage.input.partitions) * stage.input.size
            )
            w = dist.weights(stage.output.distribution, stage.output.partitions)
            stage.output.splits = ((stage.input.splits * np.array(stage.output.ratio))[:, None]) * w
            stage.tasks = [
                Task(index=i, status="pending", stage=stage) for i in range(stage.input.partitions)
            ]
        else:
            collapsed = np.sum(
                [
                    ratio * dag[dep].output.splits.sum(axis=0)
                    for ratio, dep in zip(stage.output.ratio, stage.deps)
                ],
                axis=0,
            )
            w = dist.weights(stage.output.distribution, stage.output.partitions)
            stage.output.splits = collapsed[:, None] * w
            stage.tasks = [
                Task(index=i, status="pending", stage=stage) for i in range(stage.output.partitions)
            ]
    return dag
