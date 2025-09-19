class FetchFailedException(Exception):
    def __init__(self, dep: int):
        self.dep = dep
