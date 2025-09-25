import simpy
import random
import json


class LaunchTask:
    def __init__(self, task, stage):
        self.task = task
        self.stage = stage

    def __repr__(self):
        return f"LaunchTask(id={self.task['id']}, index={self.task['index']}, stage_id={self.stage['id']}, deps={self.stage['deps']})"


class KillTask:
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return f"KillTask(id={self.id})"


class StatusUpdate:
    def __init__(self, launchref: LaunchTask, status):
        self.launchref = launchref
        self.status = status

    def __repr__(self):
        return f"StatusUpdate(id={self.launchref.task['id']}, index={self.launchref.task['index']}, stage_id={self.launchref.stage['id']}, status={self.status})"


E = 1
cores = 1
env = simpy.Environment()
scheduler_queue = simpy.Store(env)
DAG = json.load(open("dag2.json"))
for stage in DAG:
    stage["tasks"] = [
        {"index": i, "status": "pending"} for i in range(stage["partitions"])
    ]


def launch_task(msg: LaunchTask, executor_queue):
    yield env.timeout(msg.stage["stats"]["avg"])
    executor_queue.put(StatusUpdate(msg, "completed"))


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
        "instance": env.process(executor(id, (queue := simpy.Store(env)))),
        "queue": queue,
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
                if task["status"] == "pending":
                    acc.append([stage, task])
    return acc


def next_available_executor():
    for _, executor in executors.items():
        if executor["cores"] > 0:
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
                }
            )
            yield executor["queue"].put(LaunchTask(task.copy(), stage.copy()))
            executor["cores"] -= 1
        event = yield scheduler_queue.get()
        match event:
            case StatusUpdate() as status_update:
                DAG[status_update.launchref.stage["id"]]["tasks"][
                    status_update.launchref.task["index"]
                ]["status"] = status_update.status
                if all(
                    task["status"] == "completed"
                    for task in DAG[status_update.launchref.stage["id"]]["tasks"]
                ):
                    DAG[status_update.launchref.stage["id"]]["status"] = "completed"
                executors[status_update.launchref.task["executor-id"]]["cores"] += 1


env.process(scheduler())
start_time = env.now
env.run()
end_time = env.now
print(f"Total time taken: {end_time - start_time}")


"""
send a task to an executor with id, index, stage_id, deps
deps, which is just a list of stage ids, is used to determine if the executors
where the tasks of the deps ran are still running.
when executors return status update, scheduler would like to mark the status of the task as complete.
for it, it needs to know the stage id. or, if a task id is unique, we can maintain a mapping from id->stage id
as task complete, a stage might complete. mark its status appropriately.

"""
