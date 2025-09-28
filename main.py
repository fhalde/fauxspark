import simpy
import json


class LaunchTask:
    __match_args__ = ("task",)

    def __init__(self, task):
        self.task = task

    def __repr__(self):
        return f"LaunchTask(id={self.task['id']}, index={self.task['index']}, stage_id={self.task['stage']['id']})"


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
        return f"StatusUpdate(id={self.launch_task_ref.task['id']}, index={self.launch_task_ref.task['index']}, stage_id={self.launch_task_ref.task['stage']['id']}, status={self.status})"


class FetchFailed:
    __match_args__ = ("launch_task_ref", "dep")

    def __init__(self, launch_task_ref: LaunchTask, dep):
        self.launch_task_ref = launch_task_ref
        self.dep = dep

    def __repr__(self):
        return f"FetchFailed(id={self.launch_task_ref.task['id']}, index={self.launch_task_ref.task['index']}, stage_id={self.launch_task_ref.task['stage']['id']}, dep={self.dep})"


class RegisterExecutor:
    __match_args__ = ("executor_id", "spec")

    def __init__(self, executor_id, spec):
        self.executor_id = executor_id
        self.spec = spec

    def __repr__(self):
        return f"AddExecutor(executor_id={self.executor_id})"


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
    yield env.timeout(DAG[task.task["stage"]["id"]]["stats"]["avg"])
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


executors = {}


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
    running_tasks = set()

    def nextid():
        nonlocal taskid
        taskid += 1
        return taskid

    while True:
        while (executor := next_available_executor()) and (runnable_tasks:= schedulable_tasks()) != []:
            stage, task = runnable_tasks.pop()
            stage["status"], task["status"] = "running", "running"
            task = task | {
                "id": (id := nextid()),
                "executor_id": executor["id"],
                "stage": {
                    "id": stage["id"],
                    "deps": stage["deps"],
                },
            }
            running_tasks.add(task["id"])
            executor["running_tasks"][task["id"]] = task
            yield executor["queue"].put(LaunchTask(task))
            executor["available_slots"] -= 1
        event = yield scheduler_queue.get()
        match event:
            case RegisterExecutor(executor_id, spec):
                print(f"{env.now}: register executor {executor_id}")
                executors[executor_id] = spec
            case KillExecutor(executor_id):
                print(f"{env.now}: kill executor {executor_id}")
                executor = executors[executor_id]
                for tasks in executor["running_tasks"].values():
                    DAG[tasks["stage"]["id"]]["tasks"][tasks["index"]]["status"] = "killed"
                    running_tasks.remove(tasks["id"])
                del executors[executor_id]
            case FetchFailed(LaunchTask(task), dep):
                stage = task["stage"]
                # always reset the current stage.
                DAG[stage["id"]]["status"] = "pending"
                for task in DAG[stage["id"]]["tasks"]:
                    task["status"] = "pending"
                    # send KillTask message
                # mark deps as failed (all or some?)
                for dep in stage["deps"]:
                    DAG[dep]["status"] = "failed"
                    for task in DAG[dep]["tasks"]:
                        if task["executor_id"] not in executors:
                            task["status"] = "pending"
                print(f"{env.now}: fetch failed {task['id']} {dep}")
            case StatusUpdate(LaunchTask(task), "completed"):
                print(f"{env.now}: status update {task['id']} completed")
                if task["id"] not in running_tasks:
                    print(f"{env.now}: status update {task['id']} completed but not in running tasks")
                    continue
                # update task status
                stage = task["stage"]
                DAG[stage["id"]]["tasks"][task["index"]]["status"] = "completed"
                # update stage status
                if all(task["status"] == "completed" for task in DAG[stage["id"]]["tasks"]):
                    DAG[stage["id"]]["status"] = "completed"
                executor = executors[task["executor_id"]]
                executor["available_slots"] += 1
                executor["running_tasks"].pop(task["id"])
                running_tasks.remove(task["id"])

def monitor():
    yield env.timeout(1)
    # kill executor 0
    scheduler_queue.put(KillExecutor(0))
    yield env.timeout(5)
    # a new one is registered after 5 seconds
    scheduler_queue.put(RegisterExecutor(1, {
        "id": 1,
        "cores": cores,
        "available_slots": cores,
        "instance": env.process(executor(1, (queue := simpy.Store(env)))),
        "queue": queue,
        "running_tasks": {},
    }))


env.process(scheduler())

for i in range(E):
    scheduler_queue.put(RegisterExecutor(i, {
        "id": i,
        "cores": cores,
        "available_slots": cores,
        "instance": env.process(executor(i, (queue := simpy.Store(env)))),
        "queue": queue,
        "running_tasks": {},
    }))

env.process(monitor())
start_time = env.now
env.run()
end_time = env.now
print(f"Total time taken: {end_time - start_time}")
