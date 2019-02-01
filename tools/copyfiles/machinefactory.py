from misc.singleton import Singleton
from config import Config
from machine import Machine


@Singleton
class MachineFactory:
    """ Factory-class that creates the 'machine' objects in order to comply with OO-design pattern: -don't use
    what you created-. """

    def __init__(self):
        """ Default constructor. """
        self._config = Config.instance()
        self._machines = [Machine(m) for m in self._config.machines]

    @property
    def machines(self):
        """ @property-decorated method that retrieves the created machine objects."""
        return self._machines

