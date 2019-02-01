import shutil
import os
import datetime
import utils
import config
import re
from sourceshandlersfactory import SourcesHandlersFactory


class Machine:
    """ This class represents the machine for which data must be copied. It contains 1 interface
    # method (copy) that should be used by the client."""

    def __init__(self, name):
        """ default constructor """
        self._name = name

    @property
    def name(self):
        """ getter to retrieve the name of a machine, tagged with a decorator to be used as a property """
        return self._name

    def copy(self, no_of_days, is_incremental):
        """ interface-method of this class that copies the machine's data.

        Args:
            no_of_days: The number of days that should be copied. If only 1 day (today's data) should be
              copied, then 1 should be specified
            is_incremental: Only files that do not exist already in the destination will be copied
        Returns:
            -
        Raises
            Exceptions that are thrown, are not caught as they will implicate an IO-error most of the
              times that should be investigated
        """

        output_machine_path = os.path.join(config.CONFIG_OUTPUT_PATH, self.name)
        print("Copying data of machine: " + self._name)
        if not os.path.exists(output_machine_path):
            print("   Creating directory")
            os.mkdir(output_machine_path)
        for d_diff in range(0, no_of_days):
            Machine.copy_day(self.name, output_machine_path, is_incremental,
                             datetime.date.today() - datetime.timedelta(days=d_diff))

    @staticmethod
    def copy_day(machine, output_machine_path, is_incremental, the_date):
        """ Copy the data of the day that is specified by the parameter 'the_date'.

        Args:
            machine: The name of the machine
            output_machine_path: The constructed path of the output location of the machine
            is_incremental: Only files that do not exist already in the destination will be copied
            the_date: the date that will be copied
        Returns:
            -
        Raises
            Exceptions that are thrown, are not caught as they will implicate an IO-error most of the
              times that should be investigated
        """

        formatted_day = utils.get_formatted_day(the_date)
        formatted_month = utils.get_formatted_month(the_date)
        print("   Copying data of day: " + formatted_day)
        output_month_path = os.path.join(output_machine_path, formatted_month)
        if not os.path.exists(output_month_path):
            os.mkdir(output_month_path)
        output_day_path = os.path.join(output_month_path, formatted_day)
        if not os.path.exists(output_day_path):
            os.mkdir(output_day_path)
        input_day_path = os.path.join(
            os.path.join(os.path.join(
                config.CONFIG_INPUT_PATH, machine), formatted_month), formatted_day)
        if os.path.exists(input_day_path):
            # Only copy if data of the running date is already available on the source
            Machine.copy_directories(input_day_path, output_day_path, is_incremental)

    @staticmethod
    def copy_directories(input_day_path, output_day_path, is_incremental):
        """ Copy the data of all directories for the running day that.

        Args:
            input_day_path: the constructed path of the input location of the day of the machine
            output_day_path: the constructed path of the output location of the day of the machine
            is_incremental: Only files that do not exist already in the destination will be copied
        Returns:
            -
        Raises
            Exceptions that are thrown, are not caught as they will implicate an IO-error most of the
              times that should be investigated
        """

        for source_handler in SourcesHandlersFactory.instance().sources_handlers:
            for directory in source_handler["directories"]:
                Machine.copy_directory(directory, input_day_path,
                                       output_day_path, is_incremental, source_handler["file_regex"])

    @staticmethod
    def copy_directory(directory, input_day_path, output_day_path, is_incremental, file_regex):
        """ Copy the data of all directories for the running day that.

        Args:
            directory: the directory that should be copied
            input_day_path: the constructed path of the input location of the day of the machine
            output_day_path: the constructed path of the output location of the day of the machine
            is_incremental: Only files that do not exist already in the destination will be copied
            file_regex: the regular expression that should be applied to retrieve files that will be copied
        Returns:
            -
        Raises
            Exceptions that are thrown, are not caught as they will implicate an IO-error most of the
              times that should be investigated
        """

        print("       Directory: " + directory)
        directory = str(directory).replace("/", "\\")
        if not os.path.exists(os.path.join(output_day_path, directory)):
            os.makedirs(os.path.join(output_day_path, directory))
        for f in [f for f in os.listdir(os.path.join(input_day_path, directory)) if re.search(file_regex, f)]:
            destination_file = os.path.join(output_day_path, directory, f)
            if not os.path.exists(destination_file) or not is_incremental:
                src = os.path.join(input_day_path, directory, f)
                if not os.path.exists(destination_file):
                    print("          Copying not existing: " + destination_file)
                elif not is_incremental:
                    print("          Overwriting existing: " + destination_file)
                shutil.copyfile(src, destination_file)
            else:
                print("          Ignoring file: " + destination_file)

