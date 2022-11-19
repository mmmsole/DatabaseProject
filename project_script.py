import mysql.connector
import pandas as pd
from mysql.connector import errorcode

db_name = 'Formula1_database'
results = pd.read_csv('archive/results.csv')
results = results.iloc[:, 0:13]

results = results.drop( columns = 'positionText')

# we start by using the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Tazzadargento_90",
  database= db_name
)

mycursor = mydb.cursor()

#create tables
TABLES = {}

TABLES['Circuits'] = (
        '''CREATE TABLE CIRCUITS (
          circuitId INT PRIMARY KEY,
          circuitRef VARCHAR(40),
          name_ VARCHAR(80),
          location VARCHAR(100),
          country VARCHAR(80),
          lat INT,
          lng INT,
          alt INT,
          url VARCHAR(200))
        ''')

TABLES['Drivers'] = (
        '''CREATE TABLE DRIVERS (
          driverId INT PRIMARY KEY,
          driverRef VARCHAR(20) UNIQUE NOT NULL,
          number INT NOT NULL,
          code VARCHAR(10),
          name VARCHAR(80),
          surname VARCHAR(80),
          dateOfBirth DATE,
          nationality VARCHAR(80))
        ''')


TABLES['Races'] = (
        '''CREATE TABLE RACES (
          raceId INT PRIMARY KEY,
          raceYear YEAR,
          raceNumber INT,
          circuitId INT,
          raceName VARCHAR(80),
          raceDate DATE,
          raceTime TIME,
          CONSTRAINT RACES__ibfk_4 FOREIGN KEY (circuitId)
                REFERENCES CIRCUITS (circuitId) ON DELETE CASCADE)
        ''')


TABLES['Results'] = (
        '''CREATE TABLE RESULTS (
          resultId INT PRIMARY KEY,
          raceId INT,
          driverId INT,
          number INT NOT NULL,
          gridPosition INT NOT NULL,
          points INT NOT NULL,
          laps INT NOT NULL,
          timems INT NOT NULL,
          numFastLap INT NOT NULL,
          timeFastLap TIME NOT NULL,
          speedFastLap INT NOT NULL,
          CONSTRAINT RESULTS_ibfk_2 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT RESULTS_ibfk_3 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')


TABLES['DriverStandings'] = (
        '''CREATE TABLE DRIVERSTANDINGS (
          driverStandingsId INT,
          raceId INT,
          driverId INT,
          points INT,
          champPosition INT,
          numberWins INT,
          CONSTRAINT DRIVERSTANDINGS_ibfk_2 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT DRIVERSTANDINGS_ibfk_3 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')


TABLES['Pitstop'] = (
        '''CREATE TABLE PITSTOP (
          raceId INT,
          driverId INT,
          stopNumber INT PRIMARY KEY,
          lapNumber INT,
          timePitstop TIME,
          duration FLOAT,
          CONSTRAINT PITSTOP_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT PITSTOP_ibfk_2 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')


for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        mycursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

mycursor.close()
mydb.close()

