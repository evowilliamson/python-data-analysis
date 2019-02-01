from tools.ondemand.config import Config
from tools.ondemand.input_source import InputSource
from tools.ondemand.dao.dao import Dao


def sync():
    config = Config.instance()
    config.load()
    dao = Dao.instance()
    input_source = InputSource.instance()
    requests = input_source.requests
    dao.synch(requests)


# sync()
