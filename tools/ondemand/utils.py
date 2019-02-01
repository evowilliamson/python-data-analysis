from datetime import datetime as dt
import datetime
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from config import CONFIG_DIR, LOGO, ROOT

""" Library that contains utility methods """


def get_text(element):
    """
    Returns the text of the tag

    :param element: the element
    :return: the text under the tag
    """

    if element:
        return element.getText().strip()

    return None


def get_timestamp(element, __format):
    """
    Returns the timestamp of the context of a tag accroding to the passed format

    :param element: the element
    :param __format: the format of the timestamp
    :return: the parsed timestamp
    """

    if element:
        text = element.getText().strip()
        if text:
            return round_seconds(dt.strptime(text, __format))
        else:
            return None

    return None


def get_formatted_day(date):
    """ get the day formatted according to the pattern %y%m%d.

    Args:
        date: object that represents a certain date
    Returns:
        The formatted date
    """
    return date.strftime("%y%m%d")


def get_formatted_month(date):
    """ get the day formatted according to the pattern %y%m.

    Args:
        date: object that represents a certain date
    Returns:
        The formatted date
    """
    return date.strftime("%y%m")


def round_seconds(__datetime):
    """
    Method that rounds to the second
    :param __datetime: the datetime object
    :return: the datetime object rounded to the second
    """
    new_date_time = __datetime

    if new_date_time.microsecond >= 500000:
        new_date_time = new_date_time + datetime.timedelta(seconds=1)

    return new_date_time.replace(microsecond=0)


def send_mail(subject, body, recipient, sender="no_reply@test.com"):
    """
    Method that sends an email with multiparts
    :param subject: the subject of the email
    :param body: the body of the mail
    :param recipient: the recipient
    :param sender: the esender
    :return:
    """

    message = MIMEMultipart()
    message['Subject'] = subject
    message['To'] = recipient
    message['From'] = sender
    attachment = os.path.join(ROOT, CONFIG_DIR, LOGO)
    html_body = MIMEText('<b>%s</b><br><img src="cid:%s"><br>' % (body, attachment), 'html')
    message.attach(html_body)
    fp = open(attachment, 'rb')
    img = MIMEImage(fp.read())
    fp.close()
    img.add_header('Content-ID', '<{}>'.format(attachment))
    message.attach(img)
    s = smtplib.SMTP(host="mailhost.test.com", port=25)
    s.sendmail(sender, [recipient], message.as_string())
    s.quit()
