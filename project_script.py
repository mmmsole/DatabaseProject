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

# Modifiche fatte:
'''
TABELLA Circuits:
- eliminata colonna latitudine (INT)
- eliminata colonna longitudine (INT)
- eliminata colonna altitudine (INT)
- eliminata colonna url (VARCHAR(200))
'''
TABLES['Circuits'] = (
        '''CREATE TABLE CIRCUITS (
          circuitId INT PRIMARY KEY,
          circuitRef VARCHAR(40),
          name VARCHAR(80),
          location VARCHAR(100),
          country VARCHAR(80)
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
          CONSTRAINT RACES_ibfk_4 FOREIGN KEY (circuitId)
                REFERENCES CIRCUITS (circuitId) ON DELETE CASCADE)
        ''')

#THIS TABLE REPRESENTS A RELATIONSHIP (MANY TO MANY)
#Modifiche fatte
'''
TABELLA Results:
- eliminata colonna number: number INT NOT NULL
- eliminata colonna laps: laps INT NOT NULL
- da aggiungere tre colonne al diagramma: numFastLap, timeFastLap, speedFastLap
'''
TABLES['Results'] = (
        '''CREATE TABLE RESULTS (
          resultId INT PRIMARY KEY,
          raceId INT,
          driverId INT,
          gridPosition INT NOT NULL,
          finalPos INT NOT NULL
          points INT NOT NULL,
          numFastLap INT NOT NULL,
          timeFastLap TIME NOT NULL,
          speedFastLap INT NOT NULL,
          CONSTRAINT RESULTS_ibfk_2 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT RESULTS_ibfk_3 FOREIGN KEY (driverId)
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


#Aggiungo tabella LapTimes
TABLES['LapTimes'] = (
        '''CREATE TABLE LAPTIMES (
          raceID INT PRIMARY KEY,
          driverID INT PRIMARY KEY,
          lap INT PRIMARY KEY,
          position INT,
          time time,
          ms INT,
          CONSTRAINT LAPTIMES_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT LAPTIMES_ibfk_2 FOREIGN KEY (driverId)
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

