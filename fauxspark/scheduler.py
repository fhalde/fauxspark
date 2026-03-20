from typing import Generator
import typing
import simpy
from colorama import Fore
from functools import partial
from collections import deque
from fauxspark.executor import Executor
from .models import Task, Stage, LaunchTask, StatusUpdate, FetchFailed, ExecutorKilled, ShuffleLocation
from .logic import next_available_executor
from . import util


class Scheduler(object):
    def __init__(
        self,
        env: simpy.Environment,
        DAG: list[Stage],
        max_fetch_retries: int = 4,
    ):
        self.env = env
        self.DAG = DAG
        self.executors: dict[int, Executor] = dict()
        self.max_fetch_retries = max_fetch_retries
        # Driver-side shuffle metadata store: (stage_id, map_task_index) -> location
        self.shuffles: dict[tuple[int, int], ShuffleLocation] = {}
        self.scheduled: dict[int, LaunchTask] = dict()
        self.scheduler_queue = simpy.Store(env)
        self.nextid: Generator[int, None, None] = util.nextidgen()
        self.logger = partial(util.log, env, "scheduler")
        self.network_stats: dict[str, float] = {
            "local_bytes": 0.0,
            "intra_az_bytes": 0.0,
            "inter_az_bytes": 0.0,
            "intra_az_time_s": 0.0,
            "inter_az_time_s": 0.0,
        }
        self.ready_tasks: dict[int, deque[Task]] = {stage.id: deque() for stage in DAG}
        self.ready_stages: deque[int] = deque()
        self.ready_stage_set: set[int] = set()
        self.stage_children: dict[int, list[int]] = {stage.id: [] for stage in DAG}
        # (dep_stage_id, map_index, reduce_index) -> remaining injected failures
        self.fetch_failure_injections: dict[tuple[int, int, int], int] = {}
        self.failure_stats: dict[str, int] = {
            "fetch_failures_total": 0,
            "fetch_failures_injected": 0,
        }
        self._init_runtime_state()

    def start(self: "Scheduler") -> simpy.Process:
        return self.env.process(self.loop())

    @property
    def available_executors(self: "Scheduler") -> dict[int, Executor]:
        return {
            executor.id: executor for executor in self.executors.values() if not executor.killed
        }

    def loop(self: "Scheduler") -> Generator[typing.Any, None, None]:
        while True:
            self.schedule_runnable_tasks()
            event = yield self.scheduler_queue.get()
            self.logger(f"{event!r}")
            match event:
                case Executor():
                    self.register_executor(event)

                case FetchFailed():
                    self.fetch_failed(event)

                case ExecutorKilled():
                    self.executor_killed(event)

                case StatusUpdate():
                    self.status_update(event)

                case _:
                    self.logger(f"unhandled: {event!r}")

    def schedule_runnable_tasks(self: "Scheduler") -> None:
        while executor := next_available_executor(self.available_executors):
            stage_task = self._next_runnable()
            if stage_task is None:
                return
            stage, task = stage_task
            stage.status, task.status = "running", "running"
            launch_task = LaunchTask(
                tid=(id := next(self.nextid)),
                eid=executor.id,
                task=task,
                status="running",
            )
            task.current = id
            task.attempts += 1
            task.launched_tasks[id], self.scheduled[id] = launch_task, launch_task
            util.put(executor.queue, launch_task)
            executor.reserve()

    def register_executor(self: "Scheduler", executor: Executor) -> None:
        self.executors[executor.id] = executor

    def executor_killed(self: "Scheduler", executor_killed: ExecutorKilled) -> None:
        executor = self.executors[executor_killed.eid]
        for tid in executor.taskprocs.keys():
            if launched_task := self.scheduled.pop(tid, None):
                task = launched_task.task
                task.status, task.current = "pending", None
                self._enqueue_task(task)
                launched_task.status = "killed"
        # invalidate all shuffle metadata produced by this executor
        for key, location in list(self.shuffles.items()):
            if location.executor_id == executor_killed.eid:
                self.shuffles.pop(key, None)
        # del self.executors[executor.id]

    def fetch_failed(self: "Scheduler", fetch_failed: FetchFailed) -> None:
        self.failure_stats["fetch_failures_total"] += 1
        if fetch_failed.reason == "injected shuffle fetch failure":
            self.failure_stats["fetch_failures_injected"] += 1
        launch_task = self.scheduled.pop(fetch_failed.tid, None)
        if launch_task:
            task = launch_task.task
            current_stage = task.stage
            task.fetch_failures += 1
            task.status, task.current = "pending", None
            self._enqueue_task(task)
            if all(t.status == "completed" for t in current_stage.tasks):
                current_stage.status = "completed"
            else:
                current_stage.status = "pending"
            if fetch_failed.reason != "injected shuffle fetch failure":
                parent_stage = self.DAG[fetch_failed.dep]
                parent_stage.status = "failed"
                for parent_task in parent_stage.tasks:
                    key = (parent_stage.id, parent_task.index)
                    location = self.shuffles.get(key)
                    if location is None or location.executor_id not in self.available_executors:
                        parent_task.status, parent_task.current = "pending", None
                        self._enqueue_task(parent_task)
            executor = self.executors.get(launch_task.eid, None)
            if executor:
                executor.release()
            if task.fetch_failures > self.max_fetch_retries:
                task.status = "failed"
                current_stage.status = "failed"
                self.logger(
                    f"task [{task.stage.id}-{task.index}] exhausted retries after fetch failure"
                )
        else:
            self.logger(f"{Fore.MAGENTA}stale {fetch_failed!r}")

    def status_update(self: "Scheduler", status_update: StatusUpdate) -> None:
        launched_task = self.scheduled.pop(status_update.tid, None)
        if launched_task and launched_task.task.current == status_update.tid:
            task = launched_task.task
            match status_update.status:
                case "completed":
                    task.status, task.current = "completed", status_update.tid
                    launched_task.status = "completed"
                    stage = task.stage
                    if stage.output and stage.output.shuffle:
                        executor = self.executors.get(launched_task.eid, None)
                        if executor is not None:
                            self.shuffles[(stage.id, task.index)] = ShuffleLocation(
                                stage_id=stage.id,
                                map_index=task.index,
                                executor_id=executor.id,
                                az=executor.az,
                            )
                    if all(task.status == "completed" for task in stage.tasks):
                        stage.status = "completed"
                        for child in self.stage_children[stage.id]:
                            self._maybe_enqueue_stage(child)
                    executor = self.executors.get(launched_task.eid, None)
                case "killed":
                    task.status, task.current = "pending", None
                    self._enqueue_task(task)
                    launched_task.status = "killed"
                    stage = task.stage
                    executor = self.executors.get(launched_task.eid, None)
            if executor:
                executor.release()
        else:
            self.logger(f"{Fore.MAGENTA}stale {status_update!r}")

    def _init_runtime_state(self: "Scheduler") -> None:
        for stage in self.DAG:
            for dep in stage.deps:
                self.stage_children[dep].append(stage.id)
            for task in stage.tasks:
                if task.status == "pending":
                    self.ready_tasks[stage.id].append(task)
        for stage in self.DAG:
            self._maybe_enqueue_stage(stage.id)

    def _stage_deps_completed(self: "Scheduler", stage: Stage) -> bool:
        return all(self.DAG[dep].status == "completed" for dep in stage.deps)

    def _maybe_enqueue_stage(self: "Scheduler", stage_id: int) -> None:
        stage = self.DAG[stage_id]
        if stage_id in self.ready_stage_set:
            return
        if not self._stage_deps_completed(stage):
            return
        if not self.ready_tasks[stage_id]:
            return
        self.ready_stages.append(stage_id)
        self.ready_stage_set.add(stage_id)

    def _enqueue_task(self: "Scheduler", task: Task) -> None:
        self.ready_tasks[task.stage.id].append(task)
        self._maybe_enqueue_stage(task.stage.id)

    def _next_runnable(self: "Scheduler") -> tuple[Stage, Task] | None:
        while self.ready_stages:
            stage_id = self.ready_stages.popleft()
            self.ready_stage_set.discard(stage_id)
            stage = self.DAG[stage_id]
            if not self._stage_deps_completed(stage):
                continue
            queue = self.ready_tasks[stage_id]
            while queue:
                task = queue.popleft()
                if task.status == "pending":
                    if any(t.status == "pending" for t in queue):
                        self._maybe_enqueue_stage(stage_id)
                    return (stage, task)
        return None

    def inject_fetch_failure(
        self: "Scheduler",
        dep_stage_id: int,
        map_index: int,
        reduce_index: int,
        count: int = 1,
    ) -> None:
        key = (dep_stage_id, map_index, reduce_index)
        self.fetch_failure_injections[key] = self.fetch_failure_injections.get(key, 0) + count
        self.logger(f"activated fetch-failure injection key={key} count={count}")

    def consume_injected_fetch_failure(
        self: "Scheduler", dep_stage_id: int, map_index: int, reduce_index: int
    ) -> bool:
        key = (dep_stage_id, map_index, reduce_index)
        remaining = self.fetch_failure_injections.get(key, 0)
        if remaining <= 0:
            return False
        if remaining == 1:
            self.fetch_failure_injections.pop(key, None)
        else:
            self.fetch_failure_injections[key] = remaining - 1
        return True

    def record_transfer(
        self: "Scheduler",
        size_bytes: float,
        source_az: str,
        dest_az: str,
        transfer_time_s: float,
    ) -> None:
        if source_az == dest_az:
            self.network_stats["intra_az_bytes"] += size_bytes
            self.network_stats["intra_az_time_s"] += transfer_time_s
        else:
            self.network_stats["inter_az_bytes"] += size_bytes
            self.network_stats["inter_az_time_s"] += transfer_time_s
