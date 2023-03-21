import logging
import os

from task1.classes.database import Database
from task1.classes.file import File


class Main:
    def __init__(self):
        try:
            if os.path.exists('logger.log'):
                os.remove('logger.log')
            logging.basicConfig(filename='logger.log',
                                level=logging.INFO,
                                format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
            print("\nThis application can write data from files to a database with a specific structure.\n"
                  "This application knows 4 types of request. Query result will be in the JSON.\n")
            print("Please input correct path to rooms.json!\n")
            self.rooms_path = input()
            print("Please input correct path to students.json!\n")
            self.students_path = input()
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
            print('Queries:\n'
                  '1 - List of rooms and number of students in each room\n'
                  '2 - 5 rooms with the smallest average age of students\n'
                  '3 - 5 rooms with the biggest difference in student age\n'
                  '4 - List of rooms where students of different sexes live\n'
                  '5 - Quit\n'
                  'Choose one:')
            number = int(input())

            if number not in [1, 2, 3, 4, 5]:
                logging.info("Didn't choose correct number of query...")
                continue
            if number == 5:
                logging.info("Quit!")
                break

            print('Select the format of the query result record file:\n'
                  'json\n'
                  'xml')
            format_file = input()

            if format_file not in ["json", "xml"]:
                logging.error("No format selected.")
                continue

            filename = Main.select_filename(number)
            self.data.result(filename, format_file)
        self.data.disconnection()

    @staticmethod
    def select_filename(num: int) -> str:
        """
        Create query filename"
        :param num: int
        :return: str
        """
        try:
            return f"select_{num}.sql"
        except Exception as e:
            logging.exception(e)
