import simpy
from collections import defaultdict

env = simpy.Environment()

fetch = defaultdict(list)


def shuffle():
    yield env.timeout(10)
    print("shuffle completed")


def thread():
    try:
        fetchprocess = env.process(shuffle())
        fetch[0].append(fetchprocess)
        yield fetchprocess
        print("thread completed")
    except simpy.Interrupt:
        print("interrupted")
        return


def killer():
    yield env.timeout(15)
    for k, v in fetch.items():
        for process in v:
            if process.is_alive:
                process.interrupt()


env.process(thread())
env.process(killer())
env.run()
