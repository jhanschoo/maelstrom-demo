import threading
from typing import Any


class Promise:
    TIMEOUT_IN_SECS = 5
    # create an object that is not equal to any other object
    WAITING = object()

    def __init__(self) -> None:
        self.cvar = threading.Condition(threading.Lock())
        self.value = Promise.WAITING

    def wait(self) -> Any:
        if self.value != Promise.WAITING:
            return self.value

        with self.cvar:
            self.cvar.wait(Promise.TIMEOUT_IN_SECS)

        if self.value == Promise.WAITING:
            raise TimeoutError("Promise not resolved in time")
        return self.value
    
    def resolve(self, value):
        with self.cvar:
            self.value = value
            self.cvar.notify_all()