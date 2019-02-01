from tools.ondemand.config import Config
from tools.ondemand.dao.dao import Dao
from tools.ondemand.email_processor import EmailProcessor


def send_mails():
    config = Config.instance()
    config.load()
    EmailProcessor.instance(Dao.instance().get_all_requests()).process()


# send_mails()

