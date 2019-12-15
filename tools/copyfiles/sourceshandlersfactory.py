from misc.singleton import Singleton
from tools.copyfiles.config import Config


@Singleton
class SourcesHandlersFactory:
    """ Factory class that creates the sources and handlers from the configuration, conveniently grouped together."""

    def __init__(self):
        """ Default constructor that gets the sources and handlers from a configuration. """
        self._config = Config.instance()
        self._sourceshandlers = self._config.sources_handlers

    @property
    def sources_handlers(self):
        """ @property-decorated method that retrieves the sources and handlers objects."""
        return self._sourceshandlers
