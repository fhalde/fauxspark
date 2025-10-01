from colorama import Style, Fore
import simpy


def log(env, component, msg):
    print(f"{Style.BRIGHT}{Fore.RED}{env.now:6.2f}{Style.RESET_ALL}: [{component:<12}] {msg} ")


def nextidgen():
    taskid = 0
    while True:
        yield taskid
        taskid += 1


def put(q: simpy.Store, event):
    q.put(event)
