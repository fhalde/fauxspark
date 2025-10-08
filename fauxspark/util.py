from typing import Any, Generator
from colorama import Style, Fore
from pydantic import TypeAdapter
import simpy
import numpy as np
from fauxspark import dist
from fauxspark.models import Stage, Task

LOG = True


def log(env: simpy.Environment, component: str, msg: str) -> None:
    hours = int(env.now // 3600)
    minutes = int((env.now % 3600) // 60)
    seconds = int(env.now % 60)
    time = f"{hours:02}:{minutes:02}:{seconds:02}"
    if LOG:
        print(f"{Style.BRIGHT}{Fore.RED}{time}{Style.RESET_ALL}: [{component:<12}] {msg} ")


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
            if stage.output.shuffle:
                w = dist.weights(stage.output.distribution, stage.output.partitions)
                stage.output.splits = ((stage.input.splits * np.array(stage.ratio))[:, None]) * w
            else:
                stage.output.splits = stage.input.splits * np.array(stage.ratio)
            stage.tasks = [
                Task(index=i, status="pending", stage=stage) for i in range(stage.input.partitions)
            ]
            # print(
            #     f"s={stage.id} input shape: {stage.input.splits.shape} output shape: {stage.output.splits.shape}"
            # )
        else:
            partitions = dag[stage.deps[0]].output.partitions
            accumulated = np.sum(
                [
                    ratio * dag[dep].output.splits.sum(axis=0)
                    for ratio, dep in zip(stage.ratio, stage.deps)
                ],
                axis=0,
            )
            if stage.output.shuffle:
                w = dist.weights(stage.output.distribution, stage.output.partitions)
                stage.output.splits = accumulated[:, None] * w
            else:
                stage.output.splits = accumulated
            # print(
            #     f"s={stage.id} input shape: {accumulated.shape} output shape: {stage.output.splits.shape}"
            # )
            stage.tasks = [Task(index=i, status="pending", stage=stage) for i in range(partitions)]
    return dag
