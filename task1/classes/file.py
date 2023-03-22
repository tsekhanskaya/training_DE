import json
import logging


class File:
    def __init__(self, students_file_path: str, rooms_file_path: str):
        self.students_file_path = students_file_path
        self.rooms_file_path = rooms_file_path
        logging.info('Created instance class File!')

    def read_rooms(self) -> list:
        """
        Open rooms.json. Parser data to rooms(:list)
        :return: list
        """
        try:
            with open(self.rooms_file_path, 'r') as f:
                rooms = json.load(f)
        except Exception as e:
            logging.error(e)
        return rooms

    def read_students(self) -> list:
        """
        Open students.json. Parser data to students(:list)
        :return: list
        """
        try:
            with open(self.students_file_path, 'r') as f:
                students = json.load(f)
        except Exception as e:
            logging.error(e)
        return students
