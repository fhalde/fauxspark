import simpy
from typing import Mapping
from .models import Stage, LaunchTask, StatusUpdate, FetchFailed, KillTask
from . import util
from functools import partial
from colorama import Fore, Style


class Executor(object):
    def __init__(
        self,
        env: simpy.Environment,
        DAG: list[Stage],
        executors: Mapping[int, "Executor"],
        id: int,
        cores: int,
        queue: simpy.Store,
        scheduler_queue: simpy.Store,
    ):
        self.env = env
        self.DAG = DAG
        self.id = id
        self.executors = executors
        self.cores = cores
        self.available_slots = cores
        self.logger = partial(util.log, env, f"executor-{self.id}")
        self.queue = queue
        self.scheduler_queue = scheduler_queue
        self.taskprocs: dict[int, simpy.Process] = dict()
        self.fetchprocs: dict[int, simpy.Process] = dict()

    def start(self):
        return self.env.process(self.loop())

    def loop(self):
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

    def taskproc(self, launch_task: LaunchTask):
        tid = launch_task.tid
        stage_id = launch_task.stage_id
        try:
            deps = self.DAG[stage_id].deps
            for dep in deps:
                if self.DAG[dep].status != "completed":
                    self.queue.put(FetchFailed(tid=tid, dep=dep, eid=self.id))
                    return
                for task in self.DAG[dep].tasks:
                    current = task.launched_tasks.get(task.current, None)
                    if current and (executor := self.executors.get(current.eid, None)):
                        if current.eid == self.id:  # local fetch
                            continue
                        try:
                            yield executor.fetch(tid, stage_id)
                        except simpy.Interrupt as e:
                            if e.cause == "disconnect":
                                self.queue.put(FetchFailed(tid=tid, dep=dep, eid=self.id))
                                return
                            return
                    else:
                        self.queue.put(FetchFailed(tid=tid, dep=dep, eid=self.id))
                        return
            yield self.env.timeout(self.DAG[stage_id].stats["avg"])
            self.queue.put(StatusUpdate(tid=tid, status="completed", eid=self.id))
        except simpy.Interrupt as e:
            if e.cause == "killed":
                self.queue.put(StatusUpdate(tid=tid, status="killed", eid=self.id))
                return

    def fetch(self, tid: int, stage_id: int):
        self.fetchprocs[tid] = self.env.process(self.fetchproc(stage_id))
        return self.fetchprocs[tid]

    def fetchproc(self, stage_id: int):
        # this will be the avg bytes read per partition from this shuffle dependency
        # bytes = self.DAG[stage_id].stats["shuffle"]["bytes"]
        # chunks = bytes // 48 * 1024 * 1024
        # rtt = 0.05  # rtt within an aws az
        # yield self.env.timeout(int(chunks * rtt))
        yield self.env.timeout(self.DAG[stage_id].stats["shuffle"]["avg"])

    def kill(self):
        for process in list(self.taskprocs.values()):
            if process.is_alive:
                process.interrupt("killed")
        for process in list(self.fetchprocs.values()):
            if process.is_alive:
                process.interrupt("disconnect")

    def reserve(self):
        self.available_slots -= 1

    def release(self):
        self.available_slots += 1

    def __repr__(self):
        return f"{Fore.GREEN}Executor{Style.RESET_ALL}(id={self.id}, cores={self.cores}, available_slots={self.available_slots})"
