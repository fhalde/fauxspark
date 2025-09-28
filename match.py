import simpy

env = simpy.Environment()
store = simpy.Store(env)
store.put(100)


def process(i):
    yield env.timeout(i)
    return i


def read(x, y):
    i = yield env.any_of([x, y])
    for i, v in i.items():
        print(i, v)


x = env.process(process(100))
y = env.process(process(200))

z = env.process(read(x, y))
env.run()
