# Training
The project is named training_de and is designed to manage the assignment of students to rooms in a training facility. The project is written in Python.

---
## Requirements
The following requirements must be met in order to run the project:

* Python 3.x version.
* Access to the students.json and rooms.json files, including knowledge of their path.
* The dependencies from requirements.txt must be installed.
* A local PostgresQL database named training must be created.
* A .env folder must be created with the following values:

```.env
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
```



* Run the start.py script in training_de/task1.


  <em>The program will ignore duplicate entries if the student or room ID has already been recorded.</em>

Usage
To run the program, execute the following command:

```bash
python training_de/task1/start.py
```
bash
Copy code
python training_de/task1/start.py
Follow the prompts provided by the console to complete the requested queries.

Syntax
The program is written in Python. The syntax for running the program is as follows:


## Files
The following files are included in the project:

```start.py```: The main script to run the program.
```rooms.json```: Contains data for all available rooms.
```students.json```: Contains data for all available students.
```requirements.txt```: A file listing all required dependencies.
## Dependencies
The project has the following dependencies, which can be installed via pip:

```
psycopg2
python-dotenv
```
To install the dependencies, run the following command:

```
pip install -r requirements.txt
```
## Database
The program requires access to a PostgresQL database named ```training```. Please ensure that this database is created and accessible before running the program.

## .env 
The .env file is used to store database connection information. The file must contain the following values:

```
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
```
Please replace "your_user" and "your_password" with the appropriate values for your system.
