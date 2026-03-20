import typing
import simpy
from typing import Generator
from .models import Stage, LaunchTask, StatusUpdate, FetchFailed, KillTask
from . import util
from functools import partial
from colorama import Fore, Style
from typing import TYPE_CHECKING
import humanfriendly as hf

if TYPE_CHECKING:
    from .scheduler import Scheduler


class Executor(object):
    def __init__(
        self,
        env: simpy.Environment,
        DAG: list[Stage],
        id: int,
        cores: int,
        az: str,
        network_bandwidth_mb_s: float,
        intra_az_latency_ms: float,
        inter_az_latency_ms: float,
        queue: simpy.Store,
        scheduler_queue: simpy.Store,
        scheduler: "Scheduler",
    ):
        self.env = env
        self.DAG = DAG
        self.id = id
        self.cores = cores
        self.az = az
        self.network_bandwidth_mb_s = network_bandwidth_mb_s
        self.intra_az_latency_ms = intra_az_latency_ms
        self.inter_az_latency_ms = inter_az_latency_ms
        self.cores_free = cores
        self.logger = partial(util.log, env, f"executor-{self.id}")
        self.queue = queue
        self.scheduler_queue = scheduler_queue
        self.scheduler = scheduler
        self.taskprocs: dict[int, simpy.Process] = dict()
        self.fetchprocs: dict[int, simpy.Process] = dict()
        self.start_time = env.now
        self.end_time = None
        self.computed = 0

    @property
    def killed(self: "Executor") -> bool:
        return self.end_time is not None

    def start(self: "Executor") -> simpy.Process:
        return self.env.process(self.loop())

    def loop(self: "Executor") -> Generator[typing.Any, None, None]:
        while True:
            event = yield self.queue.get()
            self.logger(f"{event!r}")
            match event:
                case LaunchTask(tid=tid):
                    self.taskprocs[tid] = self.env.process(self.taskproc(event))

                case StatusUpdate(tid=tid):
                    self.taskprocs.pop(tid, None)
                    self.scheduler_queue.put(event)

                case FetchFailed(tid=tid):
                    self.taskprocs.pop(tid, None)
                    self.scheduler_queue.put(event)

                case KillTask(tid=tid):
                    process = self.taskprocs.pop(tid, None)
                    if process and process.is_alive:
                        process.interrupt("killed")
                        self.scheduler_queue.put(
                            StatusUpdate(tid=tid, status="killed", eid=self.id)
                        )
                    else:
                        self.logger(f"task={tid} not found in taskprocs")
                case _:
                    self.logger(f"unhandled: {event!r}")

    def taskproc(self, launch_task: LaunchTask) -> Generator[typing.Any, None, None]:
        start_time = self.env.now
        tid = launch_task.tid
        stage = launch_task.task.stage
        try:
            input_bytes = 0
            if stage.input:
                input_bytes = stage.input.splits[launch_task.task.index]
            deps = stage.deps
            for dep in deps:
                if self.DAG[dep].status != "completed":
                    self.queue.put(
                        FetchFailed(
                            tid=tid,
                            dep=dep,
                            eid=self.id,
                            reduce_index=launch_task.task.index,
                            reason="parent stage incomplete",
                        )
                    )
                    return
                for map_task in self.DAG[dep].tasks:
                    map_index = map_task.index
                    key = (dep, map_index)
                    shuffle_location = self.scheduler.shuffles.get(key)
                    if shuffle_location and (
                        executor := self.scheduler.available_executors.get(
                            shuffle_location.executor_id, None
                        )
                    ):
                        block_bytes = self.DAG[dep].output.splits[map_index][launch_task.task.index]
                        if shuffle_location.executor_id == self.id:  # local fetch
                            self.scheduler.network_stats["local_bytes"] += block_bytes
                            input_bytes += block_bytes
                            continue
                        if self.scheduler.consume_injected_fetch_failure(
                            dep_stage_id=dep,
                            map_index=map_index,
                            reduce_index=launch_task.task.index,
                        ):
                            self.queue.put(
                                FetchFailed(
                                    tid=tid,
                                    dep=dep,
                                    eid=self.id,
                                    map_index=map_index,
                                    reduce_index=launch_task.task.index,
                                    reason="injected shuffle fetch failure",
                                )
                            )
                            return
                        try:
                            yield executor.fetch(tid, block_bytes, shuffle_location.az, self.az)
                            input_bytes += block_bytes
                        except simpy.Interrupt as e:
                            if e.cause == "disconnect":
                                self.queue.put(
                                    FetchFailed(
                                        tid=tid,
                                        dep=dep,
                                        eid=self.id,
                                        map_index=map_index,
                                        reduce_index=launch_task.task.index,
                                        reason="shuffle server executor disconnected",
                                    )
                                )
                                return
                            raise e
                    else:
                        self.queue.put(
                            FetchFailed(
                                tid=tid,
                                dep=dep,
                                eid=self.id,
                                map_index=map_index,
                                reduce_index=launch_task.task.index,
                                reason="missing shuffle metadata",
                            )
                        )
                        return
            self.logger(
                f"[{stage.id}-{launch_task.task.index}] input bytes={hf.format_size(input_bytes)}"
            )
            yield self.env.timeout(input_bytes / stage.throughput)
            self.computed += self.env.now - start_time
            self.queue.put(StatusUpdate(tid=tid, status="completed", eid=self.id))
        except simpy.Interrupt as e:
            self.computed += self.env.now - start_time
            if e.cause == "killed":
                self.queue.put(StatusUpdate(tid=tid, status="killed", eid=self.id))
                return
            raise e

    def fetch(
        self: "Executor", tid: int, size_bytes: float, source_az: str, dest_az: str
    ) -> simpy.Process:
        self.fetchprocs[tid] = self.env.process(self.fetchproc(size_bytes, source_az, dest_az))
        return self.fetchprocs[tid]

    def fetchproc(
        self: "Executor", size_bytes: float, source_az: str, dest_az: str
    ) -> Generator[typing.Any, None, None]:
        fixed_latency_s = (
            self.intra_az_latency_ms / 1000
            if source_az == dest_az
            else self.inter_az_latency_ms / 1000
        )
        transfer_s = size_bytes / (self.network_bandwidth_mb_s * 1024 * 1024)
        total_s = fixed_latency_s + transfer_s
        self.scheduler.record_transfer(
            size_bytes=size_bytes,
            source_az=source_az,
            dest_az=dest_az,
            transfer_time_s=total_s,
        )
        yield self.env.timeout(total_s)

    def kill(self: "Executor") -> None:
        for process in list(self.taskprocs.values()):
            if process.is_alive:
                process.interrupt("killed")
        for process in list(self.fetchprocs.values()):
            if process.is_alive:
                process.interrupt("disconnect")
        self.end_time = self.env.now

    def reserve(self: "Executor") -> None:
        self.cores_free -= 1

    def release(self: "Executor") -> None:
        self.cores_free += 1

    def __repr__(self: "Executor") -> str:
        return (
            f"{Fore.GREEN}Executor{Style.RESET_ALL}(id={self.id}, az={self.az}, "
            f"cores={self.cores}, available_slots={self.cores_free})"
        )
