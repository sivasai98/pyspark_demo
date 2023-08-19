import abc

class AbstractApp(abc.ABC):

    def __init__(self,env):
        self.env = env
