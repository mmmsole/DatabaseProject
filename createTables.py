import mysql.connector
import pandas as pd
from mysql.connector import errorcode

db_name = 'F1_db'

# we start by using the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="ciaociao",
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
          country VARCHAR(80))
        ''')

TABLES['Drivers'] = (
        '''CREATE TABLE DRIVERS (
          driverId INT PRIMARY KEY,
          driverRef VARCHAR(40) UNIQUE,
          number INT, 
          code VARCHAR(20),
          name VARCHAR(80),
          surname VARCHAR(80),
          dateOfBirth DATE,
          nationality VARCHAR(80))
        ''')


TABLES['Races'] = (
        '''CREATE TABLE RACES (
          raceId INT PRIMARY KEY,
          raceYear INT,
          raceNumber INT,
          circuitId INT,
          raceName VARCHAR(80),
          raceDate DATE,
          raceTime TIME,
          CONSTRAINT RACES_ibfk_4 FOREIGN KEY (circuitId)
                REFERENCES CIRCUITS (circuitId) ON DELETE CASCADE)
        ''') # check on delete cascade

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
          gridPos INT,
          finalPos INT,
          points FLOAT,
          fastLap INT,
          CONSTRAINT RESULTS_ibfk_2 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT RESULTS_ibfk_3 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')


#Aggiungo tabella LapTimes
TABLES['LapTimes'] = (
        '''CREATE TABLE LAPTIMES (
          raceId INT,
          driverId INT,
          lap INT,
          position INT,
          ms INT,
          PRIMARY KEY (raceId, driverId, lap),
          CONSTRAINT LAPTIMES_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT LAPTIMES_ibfk_2 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')  # USE SEC_TO_TIME


TABLES['Pitstop'] = (
        '''CREATE TABLE PITSTOP (
          raceId INT,
          driverId INT,
          stopNumber INT,
          lapNumber INT,
          timePitStop TIME,
          duration FLOAT,
          PRIMARY KEY (raceId, driverId, stopNumber),
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

