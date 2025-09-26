import simpy
import json


class LaunchTask:
    __match_args__ = ("task", "stage_id")

    def __init__(self, task, stage_id):
        self.task = task
        self.stage_id = stage_id

    def __repr__(self):
        return f"LaunchTask(id={self.task['id']}, index={self.task['index']}, stage_id={self.stage_id})"


class KillTask:
    __match_args__ = ("id",)

    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return f"KillTask(id={self.id})"


class StatusUpdate:
    __match_args__ = ("launch_task_ref", "status")

    def __init__(self, launch_task_ref: LaunchTask, status):
        self.launch_task_ref = launch_task_ref
        self.status = status

    def __repr__(self):
        return f"StatusUpdate(id={self.launch_task_ref.task['id']}, index={self.launch_task_ref.task['index']}, stage_id={self.launch_task_ref.stage_id}, status={self.status})"


class KillExecutor:
    __match_args__ = ("executor_id",)

    def __init__(self, executor_id):
        self.executor_id = executor_id

    def __repr__(self):
        return f"KillExecutor(executor_id={self.executor_id})"


E = 1
cores = 1
env = simpy.Environment()
scheduler_queue = simpy.Store(env)
DAG = json.load(open("dag2.json"))
for stage in DAG:
    stage["tasks"] = [{"index": i, "status": "pending"} for i in range(stage["partitions"])]


def launch_task(task: LaunchTask, executor_queue):
    yield env.timeout(DAG[task.stage_id]["stats"]["avg"])
    executor_queue.put(StatusUpdate(task, "completed"))


def executor(id, executor_queue):
    while True:
        msg = yield executor_queue.get()
        match msg:
            case LaunchTask() as task:
                print(f"{env.now}: executor {id} launch task {task}")
                env.process(launch_task(task, executor_queue))
            case StatusUpdate() as status_update:
                scheduler_queue.put(status_update)
                print(f"{env.now}: executor {id} status update {status_update}")


executors = {
    id: {
        "id": id,
        "cores": cores,
        "available_slots": cores,
        "instance": env.process(executor(id, (queue := simpy.Store(env)))),
        "queue": queue,
        "running_tasks": {},
    }
    for id in range(E)
}


def schedulable_tasks():
    acc = []
    for stage in DAG:
        if stage["status"] != "completed" and all(
            DAG[dep]["status"] == "completed" for dep in stage["deps"]
        ):
            for task in stage["tasks"]:
                if task["status"] not in ["completed", "running"]:
                    acc.append([stage, task])
    return acc


def next_available_executor():
    for _, executor in executors.items():
        if executor["available_slots"] > 0:
            return executor
    return None


def scheduler():
    taskid = 0

    def nextid():
        nonlocal taskid
        taskid += 1
        return taskid

    while True:
        runnable_tasks = schedulable_tasks()
        while (executor := next_available_executor()) and runnable_tasks != []:
            stage, task = runnable_tasks.pop(0)
            stage["status"] = "running"
            task.update(
                {
                    "id": (id := nextid()),
                    "executor-id": executor["id"],
                    "status": "running",
                    "stage": {
                        "id": stage["id"],
                        "deps": stage["deps"],
                    }
                }
            )
            task = task.copy()
            task["stage_id"] = stage["id"]
            executor["running_tasks"][task["id"]] = task
            yield executor["queue"].put(LaunchTask(task, stage["id"]))
            executor["available_slots"] -= 1
        event = yield scheduler_queue.get()
        match event:
            case KillExecutor(executor_id):
                executor = executors[executor_id]
                for tasks in executor["running_tasks"].values():
                    DAG[tasks["stage_id"]]["tasks"][tasks["index"]]["status"] = "killed"
                executor["available_slots"] = executor["cores"]
                executor["running_tasks"] = set()
            case StatusUpdate(LaunchTask(task, stage_id), status):
                executor = executors[task["executor-id"]]
                if task["id"] in executor["running_tasks"]:
                    # update task status
                    DAG[stage_id]["tasks"][task["index"]]["status"] = status
                    # update stage status
                    if all(task["status"] == "completed" for task in DAG[stage_id]["tasks"]):
                        DAG[stage_id]["status"] = "completed"
                    executors[task["executor-id"]]["available_slots"] += 1
                    del executor["running_tasks"][task["id"]]


env.process(scheduler())
start_time = env.now
env.run()
end_time = env.now
print(f"Total time taken: {end_time - start_time}")
