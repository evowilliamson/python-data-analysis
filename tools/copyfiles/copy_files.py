import sys
import time
from machinefactory import MachineFactory

SLEEP_TIME = 60*60*1  # 1 hour

""" Main module of the application."""


def get_no_days():
    """ Get the number of days to copy.

    Returns:
        Number of days that should be copied
    """
    return int(sys.argv[1])


def is_incremental():
    """ Get the flag that indicates whether incremental copying should be applied

    Returns: true if incremental copying is applied, otherwise false
    """
    return bool(sys.argv[2])


def check_arguments():
    """ Checks whether parameters have been provided

    Raises:
        System error and exists the application if not all parameters have been provided.
    """

    if len(sys.argv) < 3:
        print("Arguments: <No. days to load> <Incremental load(0|1)>")
        sys.exit(1)


# arguments:
# 1) The number of days that should be loaded, 1 meaning only load today
# 2) Incremental load, only load new files. 1: True, 0: False

if __name__ == '__main__':

    check_arguments()

    while True:
        for machine in MachineFactory.instance().machines:
            machine.copy(get_no_days(), is_incremental())
        time.sleep(SLEEP_TIME)





