import json
import logging


class File:
    """
    class File(builtins.object). Read files to list(tuple) for next processing.
   File(students_file_path: str, rooms_file_path: str)


   Methods defined here:

   __init__(self, students_file_path: str, rooms_file_path: str)
       Initialize self.  See help(type(self)) for accurate signature.

   read_rooms(self) -> list
       Open rooms.json. Parser data to rooms(:list)
       :return: list

   read_students(self) -> list
       Open students.json. Parser data to students(:list)
       :return: list

   ----------------------------------------------------------------------
   Data descriptors defined here:

   __dict__
       dictionary for instance variables (if defined)

   __weakref__
       list of weak references to the object (if defined)
    """
    def __init__(self, students_file_path: str, rooms_file_path: str):
        self.students_file_path = students_file_path
        self.rooms_file_path = rooms_file_path
        logging.info('Created instance class File!')

    def read_rooms(self) -> list:
        """
        Open rooms.json. Parser data to rooms(:list)
        :return: list
        """

        with open(self.rooms_file_path, 'r') as f:
            rooms = json.load(f)
        return rooms

    def read_students(self) -> list:
        """
        Open students.json. Parser data to students(:list)
        :return: list
        """
        with open(self.students_file_path, 'r') as f:
            students = json.load(f)
        return students
