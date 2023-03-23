# Task 1
This is a console application and is designed to manage the assignment of students to rooms in a training facility. The project is written in Python.
```
Python 3.8.10
psql (PostgreSQL) 15.2
```
---
## Requirements
The following requirements must be met in order to run the application:

* Python 3.x version.
* Access to the students.json and rooms.json files, including knowledge of their path.
* The dependencies from requirements.txt must be installed.
* A local PostgreSQL database must be created.
* A .env folder must be created with the following values:

```.env
DATABASE_HOST=your_lhost
DATABASE_NAME=your_database_name
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
```

* Run the ```start.py``` script in ```training_de/task1```.


  <em>The program will ignore duplicate entries if the student or room ID has already been recorded.</em>

## Usage
To run the program, execute the following command:

```bash
python3 training_de/task1/start.py
```
Follow the prompts provided by the console to complete the requested queries.


## Files
The following files are used in the project:

* ```requirements.txt```: A file listing all required dependencies.
* ```start.py```: The main script to run the program.
* ```rooms.json```: Contains data for all available rooms.
    ```json 
    [
        {
            "id": 0,
            "name": "Room #0"
        },
        {
            "id": 1,
            "name": "Room #1"
        }
    ]
    ```
* ```students.json```: Contains data for all available students.
    ```json lines
    [
        {
            "birthday": "2011-08-22T00:00:00.000000",
            "id": 0,
            "name": "Peggy Ryan",
            "room": 473,
            "sex": "M"
        }
    ]
    ```

## Dependencies
To install the dependencies, run the following command:
```
pip install -r requirements.txt
```
## Database
The program requires access to a PostgresQL database named ```training```. Please ensure that this database is created and accessible before running the program.

## .env 
Create a file at the root of the directory.

The ```.env``` file is used to store database connection information.

The file must contain the following values:

```
DATABASE_HOST=your_lhost
DATABASE_NAME=your_database_name
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
```
