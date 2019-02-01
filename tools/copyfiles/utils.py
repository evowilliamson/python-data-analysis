""" Utility module."""


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
