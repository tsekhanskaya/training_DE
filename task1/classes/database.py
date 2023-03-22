import json
import xml.etree.ElementTree as ET
import psycopg2
import logging
import os

from dotenv import load_dotenv

load_dotenv()


class Database:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                host='localhost',
                database='training',
                user=os.getenv('DATABASE_USER'),
                password=os.getenv('DATABASE_PASSWORD'))
            logging.info('Created the connection object')

            self.cursor = self.conn.cursor()
            logging.info('Created the cursor object')

            query_files = ['create_table_rooms.sql', 'create_table_students.sql', 'indexes.sql']
            for query_file in query_files:
                query_create_table = self.query_string(query_file)
                self.cursor.execute(query_create_table)
                logging.info(f' {query_file} created successfully')
                print(f' {query_file} created successfully')

            logging.info('Database class instance successfully created!')
        except Exception as e:
            logging.exception(e)

    def disconnection(self) -> None:
        """
        Disconnection database
        :return: None
        """
        try:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
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
            self.cursor.executemany(
                "INSERT INTO students (id, name, sex, birthday, room_id) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                rows
            )
            self.conn.commit()
            inserted_count = self.cursor.rowcount
            print(f"{inserted_count} students were uploaded successfully!")
            logging.info(f'{inserted_count} students uploaded to students table!')
        except Exception as e:
            self.conn.rollback()
            logging.exception("Error: %s", e)

    def write_rooms(self, rooms: list) -> None:
        """
        Writes the list of rooms in the current database to the rooms table
        :param rooms: list
        :return: None
        """
        try:
            rows = [(room['id'], room['name']) for room in rooms]
            self.cursor.executemany(
                "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                rows
            )
            self.conn.commit()
            inserted_count = self.cursor.rowcount
            print(f"{inserted_count} rooms were uploaded!")
        except Exception as e:
            self.conn.rollback()
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
        print(f"{absolute_file_path} has been read!")
        return query

    def result(self, query_filename: str, format_result_file: str) -> None:
        """
        :param query_filename: str
        :param format_result_file: str
        :return: None
        """
        try:
            query = Database.query_string(query_filename)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            filename_without_extension = query_filename.replace(".sql", "")
            if not os.path.exists('results'):
                os.makedirs('results')
            output_file = f"result_{filename_without_extension}.{format_result_file}"

            if format_result_file == 'json':
                self.response_json(results, output_file)
            elif format_result_file == 'xml':
                self.response_xml(results, output_file)
        except Exception as e:
            logging.exception(e)

    def response_json(self, results: list, output_file: str) -> None:
        """
        The query result is written to a result_{filename_query}.json.
        :param output_file: str
        :param results: list
        :return: None
        """
        try:
            with open(os.path.join('results', output_file), 'w') as f:
                rows = []
                for row in results:
                    rows.append(dict(zip([column[0] for column in self.cursor.description], row)))
                json.dump(rows, f, indent=1)
            logging.info(f"The result was successfully written to a {output_file}!")
        except Exception as e:
            logging.exception(e)

    def response_xml(self, results: list, output_file: str) -> None:
        """
        The query result is written to a result_{filename_query}.xml.
        :param output_file: str
        :param results: list
        :return: None
        """
        try:
            output_path = os.path.join('results', output_file)
            root = ET.Element('result')
            for row in results:
                record = ET.SubElement(root, 'record')
                for i in range(len(self.cursor.description)):
                    field = self.cursor.description[i][0]
                    value = str(row[i])
                    ET.SubElement(record, field).text = value
            tree = ET.ElementTree(root)
            tree.write(output_path, encoding='utf-8', xml_declaration=True)
            logging.info(f"The result was successfully written to a {output_file}!")
        except Exception as e:
            logging.exception(e)
