# type: ignore

import simpy


wait = 5


def proc(env: simpy.Environment):
    global wait
    at = env.now
    print(f"{env.now:.2f}: waiting for {wait:.2f}")
    while True:
        try:
            yield env.timeout(wait)
            print(f"{env.now:.2f}: ok")
            return
        except simpy.Interrupt as e:
            d = e.cause
            wait = at + d["extend"]


def killer(env: simpy.Environment, proc: simpy.Process):
    yield env.timeout(3)
    print(f"{env.now:.2f}: extending by 10")
    proc.interrupt({"extend": 10})


env = simpy.Environment()
process = env.process(proc(env))
killer = env.process(killer(env, process))
env.run()
