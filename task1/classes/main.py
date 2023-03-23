import logging
import os
import click

from task1.classes.database import Database
from task1.classes.file import File


class Main:
    """

    class Main(builtins.object). Basic operation of a console application

   Methods defined here:
   
   __init__(self)
       Initialize self.  See help(type(self)) for accurate signature.
   
   work(self) -> None
       Basic work with queries for data processing.
       :return: None
   
   ----------------------------------------------------------------------
   Data descriptors defined here:
   
   __dict__
       dictionary for instance variables (if defined)
   
   __weakref__
       list of weak references to the object (if defined)

    """
    def __init__(self):
        try:
            if os.path.exists('logger.log'):
                os.remove('logger.log')
            logging.basicConfig(filename='logger.log',
                                level=logging.INFO,
                                format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')
            click.echo('This application can write data from files to a database with a specific structure.\n'
                       'This application knows 4 types of request. Query result will be in the JSON.\n')
            self.rooms_path = click.prompt('Please input correct path to rooms.json!', type=str)
            self.students_path = click.prompt('Please input correct path to students.json!', type=str)
            self.data = Database()
        except Exception as e:
            logging.exception(e)

    def work(self) -> None:
        """
        Basic work with queries for data processing.
        :return: None
        """
        file = File(self.students_path, self.rooms_path)
        list_rooms = file.read_rooms()  # Reading all records from a file
        self.data.write_rooms(list_rooms)  # Write room data to rooms table
        list_students = file.read_students()  # Reading all records from a file
        self.data.write_students(list_students)  # Write student data to students table

        while True:
            click.echo('Queries:\n'
                       '1 - List of rooms and number of students in each room\n'
                       '2 - 5 rooms with the smallest average age of students\n'
                       '3 - 5 rooms with the biggest difference in student age\n'
                       '4 - List of rooms where students of different sexes live\n'
                       '5 - Quit\n')
            number = click.prompt('Choose one', type=click.IntRange(1, 5))

            if number == 5:
                click.echo('Quit!')
                logging.info('Quit!')
                break

            format_file = click.prompt('Enter the file format for writing query results',
                                       type=click.Choice(['json', 'xml']))

            filename = f'select_{number}.sql'
            self.data.result(filename, format_file)
        self.data.disconnection()
