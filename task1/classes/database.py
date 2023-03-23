import json
import xml.etree.ElementTree as ET
import psycopg2
import logging
import typing
import os
import click

from dotenv import load_dotenv

load_dotenv()


class Database:
    """
    class Database(builtins.object).
    The class allows you to connect and disconnect to the database.
    Writes data from previously processed rooms.json and students.json.
    Executes database queries.

   Methods defined here:

   __init__(self)
       Initialize self.  See help(type(self)) for accurate signature.

   disconnection(self) -> None
       Disconnection database
       :return: None

   response_json(self, results: list, output_file: str, description: str) -> None
       The query result is written to a result_{filename_query}.json.
       :param description: str
       :param output_file: str
       :param results: list
       :return: None

   response_xml(self, results: list, output_file: str, description: str) -> None
       The query result is written to a result_{filename_query}.xml.
       :param description: str
       :param output_file: str
       :param results: list
       :return: None

   result(self, query_filename: str, format_result_file: str) -> None
       :param query_filename: str
       :param format_result_file: str
       :return: None

   write_rooms(self, rooms: list) -> None
       Writes the list of rooms in the current database to the rooms table
       :param rooms: list
       :return: None

   write_students(self, students: list) -> None
       Writes the list of students in the current database to the students table
       :param students: list
       :return: None

   ----------------------------------------------------------------------
   Static methods defined here:

   query_string(query_file: str) -> str
       Getting the text of the request.
       :param query_file: str        :return: str

   ----------------------------------------------------------------------
   Readonly properties defined here:

   connection

   ----------------------------------------------------------------------
   Data descriptors defined here:

   __dict__
       dictionary for instance variables (if defined)

   __weakref__
       list of weak references to the object (if defined)

   ----------------------------------------------------------------------
   Data and other attributes defined here:

   __annotations__ = {'_connection': typing.Union[connect, NoneType]}
    """

    _connection: typing.Union[psycopg2.connect, None] = None

    @property
    def connection(self) -> psycopg2.connect:
        if self._connection is None:
            try:
                type(self)._connection = psycopg2.connect(
                    host=os.getenv('DATABASE_HOST'),
                    database=os.getenv('DATABASE_NAME'),
                    user=os.getenv('DATABASE_USER'),
                    password=os.getenv('DATABASE_PASSWORD'))
            except Exception as e:
                logging.error(e)
        return self._connection

    def __init__(self):
        try:
            query_files = ['create_table_rooms.sql', 'create_table_students.sql', 'indexes.sql']
            for query_file in query_files:
                query_create_table = self.query_string(query_file)
                with self.connection.cursor() as cursor:
                    cursor.execute(query_create_table)
                self.connection.commit()

                logging.info(f' {query_file} was execute successfully!')
                click.echo(f' {query_file} was execute successfully!')

            logging.info('Database class instance successfully created!')
        except Exception as e:
            logging.exception(e)

    def disconnection(self) -> None:
        """
        Disconnection database
        :return: None
        """
        try:
            self._connection.close()
            type(self)._connection = None
            logging.info('Database disconnection was successful.')
        except Exception as e:
            logging.exception("Error: %s", e)

    def write_students(self, students: list) -> None:
        """
        Writes the list of students in the current database to the students table
        :param students: list
        :return: None
        """
        try:
            rows = [(student['id'], student['name'], student['sex'], student['birthday'], student['room']) for student
                    in students]
            with self.connection.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO students (id, name, sex, birthday, room_id) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    rows
                )
                self.connection.commit()
                inserted_count = cursor.rowcount
            click.echo(f"{inserted_count} students were uploaded successfully!")
            logging.info(f'{inserted_count} students uploaded to students table!')
        except Exception as e:
            self.connection.rollback()
            logging.exception("Error: %s", e)

    def write_rooms(self, rooms: list) -> None:
        """
        Writes the list of rooms in the current database to the rooms table
        :param rooms: list
        :return: None
        """
        try:
            rows = [(room['id'], room['name']) for room in rooms]
            with self.connection.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                    rows
                )
                self.connection.commit()
                inserted_count = cursor.rowcount
            click.echo(f"{inserted_count} rooms were uploaded successfully!")
            logging.info(f'{inserted_count} rooms uploaded to students table!')
        except Exception as e:
            self.connection.rollback()
            logging.exception(e)

    @staticmethod
    def query_string(query_file: str) -> str:
        """
        Getting the text of the request.
        :param query_file: str        :return: str
        """
        current_dir = os.path.dirname(__file__)
        relative_path = f"../queries/{query_file}"
        absolute_file_path = os.path.join(current_dir, relative_path)
        with open(absolute_file_path, 'r') as f:
            query = f.read()
        click.echo(f"{query_file} has been read!")
        return query

    def result(self, query_filename: str, format_result_file: str) -> None:
        """
        :param query_filename: str
        :param format_result_file: str
        :return: None
        """
        try:
            query = Database.query_string(query_filename)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                description = cursor.description
            filename_without_extension = query_filename.replace(".sql", "")
            if not os.path.exists('results'):
                os.makedirs('results')
            output_file = f"result_{filename_without_extension}.{format_result_file}"

            if format_result_file == 'json':
                self.response_json(results, output_file, description)
            elif format_result_file == 'xml':
                self.response_xml(results, output_file, description)
        except Exception as e:
            logging.exception(e)

    def response_json(self, results: list, output_file: str, description: str) -> None:
        """
        The query result is written to a result_{filename_query}.json.
        :param description: str
        :param output_file: str
        :param results: list
        :return: None
        """
        try:
            with open(os.path.join('results', output_file), 'w') as f:
                rows = []

                for row in results:
                    rows.append(dict(zip([column[0] for column in description], row)))
                json.dump(rows, f, indent=1)
            logging.info(f"The result was successfully written to a {output_file}!")
            click.echo(f"The result was successfully written to a {output_file}!")
        except Exception as e:
            logging.exception(e)

    def response_xml(self, results: list, output_file: str, description: str) -> None:
        """
        The query result is written to a result_{filename_query}.xml.
        :param description: str
        :param output_file: str
        :param results: list
        :return: None
        """
        try:
            output_path = os.path.join('results', output_file)
            root = ET.Element('result')
            for row in results:
                record = ET.SubElement(root, 'record')

                for i in range(len(description)):
                    field = description[i][0]
                    value = str(row[i])
                    ET.SubElement(record, field).text = value
            tree = ET.ElementTree(root)
            tree.write(output_path, encoding='utf-8', xml_declaration=True)
            logging.info(f"The result was successfully written to a {output_file}!")
            click.echo(f"The result was successfully written to a {output_file}!")
        except Exception as e:
            logging.exception(e)
