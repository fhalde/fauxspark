import simpy
import random

cores = 8
env = simpy.Environment()
scheduler_queue = simpy.Store(env)


class TaskRequest:
    def __init__(self, executor_id, task):
        self.executor_id = executor_id
        self.task = task


class TaskResponse:
    def __init__(self, task):
        self.task = task


def taskrun(task, executor_queue):
    yield env.timeout(0.1)
    executor_queue.put(TaskResponse(task))


def executor(id, executor_queue):
    while True:
        event = yield executor_queue.get()
        match event:
            case TaskRequest() as task:
                print(f"{env.now}: executor {id} got task {task}")
                env.process(taskrun(task, executor_queue))
            case TaskResponse() as response:
                scheduler_queue.put(response)
                print(f"{env.now}: executor finished {response.task}")


E = 10
executors = [
    {
        "id": id,
        "cores": cores,
        "instance": env.process(executor(id, (queue := simpy.Store(env)))),
        "queue": queue,
    }
    for id in range(E)
]


def slots_available():
    for executor in executors:
        if executor["cores"] > 0:
            return executor
    return None


def scheduler():
    tasks = [i for i in range(10)]
    mapping = {}
    while True:
        while (executor := slots_available()) and tasks != []:
            task = tasks.pop(0)
            mapping[task] = executor
            yield executor.queue.put(TaskRequest(task))
            executor["cores"] -= 1
        event = yield scheduler_queue.get()
        match event:
            case TaskResponse() as response:
                executor = mapping[response.task]
                executor["cores"] += 1
                print(f"{env.now}: scheduler got result {response.task}")


env.process(scheduler())
start_time = env.now
env.run()
end_time = env.now
print(f"Total time taken: {end_time - start_time}")
