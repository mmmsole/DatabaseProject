import mysql.connector
from mysql.connector import Error, errorcode
import pandas as pd

db_name = 'F1_db'
identifier = input('Who are you?\nMariasole (1), Davide (2) or Demetrio (3)?\nIf you are someone else, enter.\n> ')

if identifier == '1':
    pw = 'Tazzadargento_90'
    user = 'Mariasole'
elif identifier == '2':
    pw = 'ciaociao'
    user = 'Davide'
elif identifier == '3':
    pw = '#MySQLDemi2022'
    user = 'Demetrio'
else:
    user = input('What is your name?\n> ')
    pw = input('Enter your password, please.\n> ')

mydb = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password=pw,
                                   auth_plugin='mysql_native_password')


def load_data():
    try:
        if mydb.is_connected():
            mycursor = mydb.cursor()
            mycursor.execute('SHOW DATABASES')
            result = mycursor.fetchall()
            for x in result:
                if db_name == x[0]:
                    mycursor.execute('DROP DATABASE ' + db_name)  # delete old database
                    mydb.commit()  # make the changes official
                    print("The database already exists! The old database has been deleted!")

            mycursor.execute("CREATE DATABASE " + db_name)
            mycursor.execute("USE " + db_name)
    except errorcode as e:
        print("Error while connecting to MySQL", e)


def execute_query(connection, query):
    mycursor = connection.cursor()
    try:
        mycursor.execute(query)
        connection.commit()
        if query == Circuits:
            print("Table 'Circuits' was created")
        elif query == Drivers:
            print("Table 'Drivers' was created")
        elif query == Results:
            print("Table 'Results' was created")
        elif query == Races:
            print("Table 'Races' was created")
        elif query == LapTimes:
            print("Table 'LapTimes' was created")
        elif query == PitStops:
            print("Table 'PitStops' was created")
    except Error as e:
        print("Error while connecting to MySQL", e)


Circuits = (
        '''CREATE TABLE CIRCUITS (
          circuitId INT PRIMARY KEY,
          circuitRef VARCHAR(40),
          name VARCHAR(80),
          location VARCHAR(100),
          country VARCHAR(80))
        ''')
Drivers = (
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
Races = (
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
        ''')
Results = (
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
LapTimes = (
        '''CREATE TABLE LAPTIMES (
          raceId INT,
          driverId INT,
          lap INT,
          position INT,
          lapTime VARCHAR(15),
          ms INT,
          PRIMARY KEY (raceId, driverId, lap),
          CONSTRAINT LAPTIMES_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT LAPTIMES_ibfk_2 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')
PitStops = (
        '''CREATE TABLE PITSTOPS (
          raceId INT,
          driverId INT,
          stopNumber INT,
          lapNumber INT,
          timePitStop TIME,
          duration FLOAT,
          PRIMARY KEY (raceId, driverId, stopNumber),
          CONSTRAINT PITSTOPS_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT PITSTOPS_ibfk_2 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE)
        ''')


circuits_1 = pd.read_csv('archive/circuitsRaw.csv')
drivers_1 = pd.read_csv('archive/driversRaw.csv')
races_1 = pd.read_csv('archive/racesRaw.csv')
results_1 = pd.read_csv('archive/resultsRaw.csv')
lap_times_1 = pd.read_csv('archive/lap_timesRaw.csv')
pit_stops_1 = pd.read_csv('archive/pit_stopsRaw.csv')

# GET ALL CIRCUITS
circuits = circuits_1.iloc[:, 0:5]

# FROM RACES GET YEAR < 2002
races = races_1.loc[races_1["year"] > 2001]
races = races.iloc[:,0:7]

# GET LAP TIMES OF OUR RACES
lap_times = lap_times_1[lap_times_1['raceId'].isin(races['raceId'])]
lap_times = lap_times.iloc[:,0:6]

# GET PIT STOPS IN OUR LAP TIMES
pit_stops = pit_stops_1[pit_stops_1['raceId'].isin(lap_times['raceId'])]
pit_stops = pit_stops.iloc[:,0:6]
for i in pit_stops['duration']:
    if len(i) > 6:
        pit_stops['duration'] = pit_stops['duration'].replace(i,r'\N')

# GET RESULTS OF OUR RACES
results = results_1[results_1['raceId'].isin(races['raceId'])]
results = results.iloc[:,[0,1,2,5,6,9,13]]

# GET DRIVERS THAT DID OUR LAP TIMES
drivers = drivers_1[drivers_1['driverId'].isin(lap_times['driverId'])]
drivers = drivers.iloc[:,:-1]

def insert_circuits():
    mycursor = mydb.cursor()
    try:
        for i, row in circuits.iterrows():
            sql = f"INSERT INTO {db_name}.CIRCUITS VALUES (%s,%s,%s,%s,%s)"
            mycursor.execute(sql,
                             tuple([row['circuitId'], row['circuitRef'], row['name'], row['location'], row['country']]))
            # print("Record inserted")
            mydb.commit()
        print('Circuits inserted')
    except Exception as e:
        if str(e)[0:4] == '1062':
            print('Circuits already inserted')
        else:
            print(e)


def insert_drivers():
    mycursor = mydb.cursor()
    try:
        for i, row in drivers.iterrows():
            sql = f"INSERT INTO {db_name}.DRIVERS VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            for x in range(len(row)):
                if row[x] == r'\N':
                    row[x] = None
            mycursor.execute(sql, tuple(
                [row['driverId'], row['driverRef'], row['number'], row['code'], row['forename'], row['surname'],
                 row['dob'], row['nationality']]))
            # print("Record inserted")
            mydb.commit()
        print('Drivers inserted')
    except Exception as e:
        if str(e)[0:4] == '1062':
            print('Drivers already inserted')
        else:
            print(e)


def insert_races():
    mycursor = mydb.cursor()
    try:
        for i, row in races.iterrows():
            sql = f"INSERT INTO {db_name}.RACES VALUES (%s,%s,%s,%s,%s,%s,%s)"
            for x in range(len(row)):
                if row[x] == r'\N':
                    row[x] = None
            mycursor.execute(sql, tuple(
                [row['raceId'], row['year'], row['round'], row['circuitId'], row['name'], row['date'], row['time']]))
            # print("Record inserted")
            mydb.commit()
        print('Races inserted')
    except Exception as e:
        if str(e)[0:4] == '1062':
            print('Races already inserted')
        else:
            print(e)


def insert_results():
    mycursor = mydb.cursor()
    try:
        for i, row in results.iterrows():
            sql = f"INSERT INTO {db_name}.RESULTS VALUES (%s,%s,%s,%s,%s,%s,%s)"
            for x in range(len(row)):
                if row[x] == r'\N':
                    row[x] = None
            mycursor.execute(sql, tuple(
                [row['resultId'], row['raceId'], row['driverId'], row['grid'], row['position'], row['points'],
                 row['fastestLap']]))
            # print("Record inserted")
            mydb.commit()
        print('Results inserted')
    except Exception as e:
        if str(e)[0:4] == '1062':
            print('Results already inserted')
        else:
            print(e)


def insert_lap_times():
    mycursor = mydb.cursor()
    try:
        for i, row in lap_times.iterrows():
            sql = f"INSERT INTO {db_name}.LAPTIMES VALUES (%s,%s,%s,%s,%s,%s)"
            for x in range(len(row)):
                if row[x] == r'\N':
                    row[x] = None
            mycursor.execute(sql, tuple(
                [str(row['raceId']), str(row['driverId']), str(row['lap']),
                 str(row['position']), str(row['time']), str(row['milliseconds'])]))
            # print("Record inserted")
            mydb.commit()
        print('Laptimes inserted')
    except Exception as e:
        if str(e)[0:4] == '1062':
            print('Laptimes already inserted')
        else:
            print(e)


def insert_pit_stops():
    mycursor = mydb.cursor()
    try:
        for i, row in pit_stops.iterrows():
            sql = f"INSERT INTO {db_name}.PITSTOPS VALUES (%s,%s,%s,%s,%s,%s)"
            for x in range(len(row)):
                if row[x] == r'\N':
                    row[x] = None
            mycursor.execute(sql, tuple(
                [row['raceId'], row['driverId'], row['stop'], row['lap'], row['time'], row['duration']]))
            # print("Record inserted")
            mydb.commit()
        print('Pitstops inserted')
    except Exception as e:
        if str(e)[0:4] == '1062':
            print('Pitstops already inserted')
        else:
            print(e)




def query1():
    mycursor = mydb.cursor()
    mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(p.stopNumber) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.driverId
        Having d.name = 'Max'  and d.surname = 'Verstappen'
        ''')
    result1 = mycursor.fetchall()

    mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(p.stopNumber) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.driverId
        Having d.name = 'Lewis'  and d.surname = 'Hamilton'
        ''')
    result2 = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result1}\n{result2}\n====\n")


def query2():
    mycursor = mydb.cursor()
    mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, r.raceName as GrandPrix, c.name as CircuitName, count(*) as NumPitStops
        From PitStops as p, Races as r, Drivers as d, Circuits as c
        Where p.raceId = r.raceId and p.driverId = d.driverId and r.circuitId = c.circuitId and r.raceYear = 2021
        Group by r.raceName, d.driverId
        Having d.name = 'Max' and d.surname = 'Verstappen'
        ''')
    result1 = mycursor.fetchall()

    mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, r.raceName as GrandPrix, c.name as CircuitName, count(*) as NumPitStops
        From PitStops as p, Races as r, Drivers as d, Circuits as c
        Where p.raceId = r.raceId and p.driverId = d.driverId and r.circuitId = c.circuitId and r.raceYear = 2021
        Group by r.raceName, d.driverId
        Having d.name = 'Lewis' and d.surname = 'Hamilton'
        ''')
    result2 = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result1}\n{result2}\n====\n")


def query3():
    mycursor = mydb.cursor()
    mycursor.execute('''Select d.driverId as DriverId, d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(p.stopNumber) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.driverId
        Having count(p.stopNumber) < (
            Select x.NumPitStops
            From (
                Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
                From PitStops as p, Drivers as d, Races as r
                Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
                Group by d.name
                Having d.name = 'Max'  and d.surname = 'Verstappen'
            ) as x
        )
        ''')
    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


def query4():
    mycursor = mydb.cursor()
    mycursor.execute('''Select r.racename as GrandPrix, d.name as DriverName, d.surname as DriverSurname, l.ms as ms, sec_to_time(ms/1000) as LapTime
        From Races as r, Drivers as d, Circuits as c, LapTimes as l
        Where r.raceId = l.raceId and d.driverId = l.driverId and r.circuitId = c.circuitId and r.raceYear = 2021
        Group by r.raceName, d.name, d.surname, l.ms, r.raceYear
        Having l.ms in (
            Select x.MinTime
            From (
                Select r.raceName, MIN(l.ms) as MinTime
                From Races as r, Drivers as d, Circuits as c, LapTimes as l
                Where r.raceId = l.raceId and d.driverId = l.driverId and r.circuitId = c.circuitId and r.raceYear = 2021
                Group by r.raceName
            ) as x
        )
        ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


def query5():
    mycursor = mydb.cursor()
    mycursor.execute('''Select  d.name, d.surname, d.number, sum(res.points) as Standings
        From Drivers as d, Results as res, Races as r
        Where d.driverId = res.driverId and r.raceId = res.raceId and r.raceYear = 2021
        Group by d.name, d.surname
        Order by Standings DESC
        ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


def query6():
    mycursor = mydb.cursor()
    mycursor.execute('''Select d.driverId as DriverId, d.name as DriverName, d.surname as DriverSurname, SUM(r.points) as TotalPoints
        From Drivers as d, Results as r
        Where d.driverId = r.driverId and d.nationality = 'Italian'
        Group by d.driverId
        Order by TotalPoints DESC
        ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")

def query7():
    mycursor = mydb.cursor()
    mycursor.execute('''Select name as Name, surname as surname
        From Drivers
        Where driverId NOT IN (
            Select distinct driverId
            From Results
            Where finalPos = 1
        )
        ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")

def query8():
    mycursor = mydb.cursor()
    mycursor.execute('''Select name, surname
        From Drivers
        Where driverId IN (
            Select driverId
            From Results
            Group by driverId
            Having SUM(points) = 0
        )
        ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


def query9():
    mycursor = mydb.cursor()
    mycursor.execute('''Select r.raceId, r.raceName, r.raceYear, AVG(p.stopNumber) as AvgPitStops
        From Races as r, PitStops as p
        where p.raceId = r.raceId and r.raceYear = 2020
        Group by r.raceId
            ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")



if __name__ == "__main__":
    print(f"\nWelcome to our project, {user}!\n")
    while True:
        valid_choices = [1,2,3,4,5]
        choice = int(input('''        MENU
        Please choose your next action by typing one among the following numbers and enter.
        1 - Load dataset and create database
        2 - Use database and create tables
        3 - Insert values into tables
        4 - Run queries
        5 - Quit
        > '''))
        if choice == 1:
            print("\nCreating database...")
            load_data()
            print('\nDatabase successfully created and used, returning to menu.\
              \n________________________________________________________________')
            continue
        if choice == 2:
            mycursor= mydb.cursor()
            print("\nStarting to create tables...\n")
            mycursor.execute("""DROP TABLE IF EXISTS license""")
            mycursor.execute("""DROP TABLE IF EXISTS holder""")
            mycursor.execute("""DROP TABLE IF EXISTS company""")
            mycursor.execute("""DROP TABLE IF EXISTS location""")
            execute_query(mydb, Circuits)
            execute_query(mydb, Drivers)
            execute_query(mydb, Races)
            execute_query(mydb, Results)
            execute_query(mydb, LapTimes)
            execute_query(mydb, PitStops)
            print('\nRequest executed successfully, returning to menu.\
              \n________________________________________________________________')
            continue
        if choice == 3:
            print('\nStarting to insert data...')
            insert_circuits()
            insert_drivers()
            insert_races()
            insert_results()
            insert_lap_times()
            insert_pit_stops()
            print('\nRequest executed successfully, returning to menu.\
              \n________________________________________________________________')
            continue
        while True:
            if choice == 4:
                valid_queries = [1,2,3,4,5,6,7,8,9]
                queries = int(input('''Choose a query to execute by typing one among the following:\n
            1 -> Total number of pitstops in 2021 season for Max Verstappen and Lewis Hamilton
            2 -> Total number of pitstops per race for Max Verstappen and Lewis Hamilton in 2021 season
            3 -> All drivers who did less pitstops than the 2021 World Champion (Max Verstappen)
            4 -> Given a year, return a list containing the driver who achieved the fastest lap on each circuit
            5 -> Drivers' standings for a given season
            6 -> Ranking according to total points of driver with a given nationality
            7 -> List of all drivers who have never won a race
            8 -> List of all drivers who have never scored points in their career in f1
            9 -> Average number of pitstops per race in a given year
            Type 'back' to go back to menu.
            > '''))
                if queries == 1:
                    query1()
                if queries == 2:
                    query2()
                if queries == 3:
                    query3()
                if queries == 4:
                    query4()
                if queries == 5:
                    query5()
                if queries == 6:
                    query6()
                if queries == 7:
                    query7()
                if queries == 8:
                    query8()
                if queries == 9:
                    query9()

                    print('\nReturning to main menu.\
                      \n________________________________________________________________')
                    break
                if queries not in valid_queries:
                    print('\nUnable to answer request: invalid input was given.\
                  \nReturning to query menu.\
                    \n________________________________________________________________')
                    break
        if choice == 5:
            print('\nApplication closed correctly\
          \n________________________________________________________________')
            break
        if choice not in valid_choices:
            print('\nUnable to answer request: invalid input was given.\
          \nReturning to menu.\n________________________________________________________________')