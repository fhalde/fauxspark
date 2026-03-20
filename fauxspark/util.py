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


def _topo_sort(stages: list[Stage]) -> list[Stage]:
    by_id: dict[int, Stage] = {s.id: s for s in stages}
    visited: set[int] = set()
    order: list[Stage] = []

    def visit(sid: int) -> None:
        if sid in visited:
            return
        visited.add(sid)
        for dep in by_id[sid].deps:
            visit(dep)
        order.append(by_id[sid])

    for s in stages:
        visit(s.id)
    return order


def init_dag(m) -> list[Stage]:
    stages = TypeAdapter(list[Stage]).validate_python(m)
    by_id: dict[int, Stage] = {s.id: s for s in stages}
    ordered = _topo_sort(stages)

    for stage in ordered:
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
        else:
            if len(stage.ratio) != len(stage.deps):
                raise ValueError(
                    f"Stage {stage.id}: len(ratio)={len(stage.ratio)} "
                    f"must equal len(deps)={len(stage.deps)}"
                )
            first_dep = by_id[stage.deps[0]]
            partitions = first_dep.output.partitions
            accumulated = np.sum(
                [
                    ratio * by_id[dep].output.splits.sum(axis=0)
                    for ratio, dep in zip(stage.ratio, stage.deps)
                ],
                axis=0,
            )
            if stage.output.shuffle:
                w = dist.weights(stage.output.distribution, stage.output.partitions)
                stage.output.splits = accumulated[:, None] * w
            else:
                stage.output.splits = accumulated
            stage.tasks = [Task(index=i, status="pending", stage=stage) for i in range(partitions)]
    return ordered
