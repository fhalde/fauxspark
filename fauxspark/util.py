from typing import Any, Generator
from colorama import Style, Fore
import simpy


def log(env: simpy.Environment, component: str, msg: str) -> None:
    print(f"{Style.BRIGHT}{Fore.RED}{env.now:6.2f}{Style.RESET_ALL}: [{component:<12}] {msg} ")


def nextidgen() -> Generator[int, None, None]:
    taskid = 0
    while True:
        yield taskid
        taskid += 1


def put(q: simpy.Store, event: Any) -> None:
    q.put(event)
