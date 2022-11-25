# read the dataset from workspace

import mysql.connector
from mysql.connector import errorcode
from Cleaning import *
#from createDB import *
#from createTables import *

db_name = 'F1_db'

#print(circuits.info())

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="ciaociao",
  database= db_name
)

mycursor = mydb.cursor()
# Insert DataFrame records one by one.

try:
    for i, row in circuits.iterrows():
        sql = f"INSERT INTO {db_name}.CIRCUITS VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple([row['circuitId'], row['circuitRef'], row['name'], row['location'], row['country']]))
        #print("Record inserted")
        mydb.commit()
    print('Circuits inserted')
except Exception as e:
    if str(e)[0:4] == '1062':
        print('Circuits already inserted')
    else:
        print(e)


try:
    for i, row in drivers.iterrows():
        sql = f"INSERT INTO {db_name}.DRIVERS VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        for x in range(len(row)):
            if row[x] == r'\N':
                row[x] = None
        mycursor.execute(sql, tuple([row['driverId'], row['driverRef'], row['number'], row['code'], row['forename'], row['surname'], row['dob'], row['nationality']]))
        #print("Record inserted")
        mydb.commit()
    print('Drivers inserted')
except Exception as e:
    if str(e)[0:4] == '1062':
        print('Drivers already inserted')
    else:
        print(e)


try:
    for i, row in races.iterrows():
        sql = f"INSERT INTO {db_name}.RACES VALUES (%s,%s,%s,%s,%s,%s,%s)"
        for x in range(len(row)):
            if row[x] == r'\N':
                row[x] = None
        mycursor.execute(sql, tuple([row['raceId'], row['year'], row['round'], row['circuitId'], row['name'], row['date'], row['time']]))
        #print("Record inserted")
        mydb.commit()
    print('Races inserted')
except Exception as e:
    if str(e)[0:4] == '1062':
        print('Races already inserted')
    else:
        print(e)


try:
    for i, row in results.iterrows():
        sql = f"INSERT INTO {db_name}.RESULTS VALUES (%s,%s,%s,%s,%s,%s,%s)"
        for x in range(len(row)):
            if row[x] == r'\N':
                row[x] = None
        mycursor.execute(sql, tuple([row['resultId'], row['raceId'], row['driverId'], row['grid'], row['position'], row['points'], row['fastestLap']]))
        #print("Record inserted")
        mydb.commit()
    print('Results inserted')
except Exception as e:
    if str(e)[0:4] == '1062':
        print('Results already inserted')
    else:
        print(e)


try:
    for i, row in lap_times.iterrows():
        row['raceId'] = str(row['raceId'])
        row['driverId'] = str(row['driverId'])
        row['lap'] = str(row['lap'])
        row['position'] = str(row['position'])
        row['milliseconds'] = str(row['milliseconds'])
        sql = f"INSERT INTO {db_name}.LAPTIMES VALUES (%s,%s,%s,%s,%s)"
        for x in range(len(row)):
            if row[x] == r'\N':
                row[x] = None
        mycursor.execute(sql, tuple([row['raceId'],
                                     row['driverId'],
                                     row['lap'],
                                     row['position'],
                                     row['milliseconds']
                                     ]))
        #print("Record inserted")
        mydb.commit()
    print('Laptimes inserted')
except Exception as e:
    if str(e)[0:4] == '1062':
        print('Laptimes already inserted')
    else:
        print(e)


try:
    for i, row in pit_stops.iterrows():
        sql = f"INSERT INTO {db_name}.PITSTOPS VALUES (%s,%s,%s,%s,%s,%s)"
        for x in range(len(row)):
            if row[x] == r'\N':
                row[x] = None
        mycursor.execute(sql, tuple([row['raceId'], row['driverId'], row['stop'], row['lap'], row['time'], row['duration']]))
        #print("Record inserted")
        mydb.commit()
    print('Pitstops inserted')
except Exception as e:
    if str(e)[0:4] == '1062':
        print('Pitstops already inserted')
    else:
        print(e)

'''sql = f"INSERT INTO F1_db.LAPTIMES VALUES(%s,%s,%s,%s,%s)"
mycursor.execute(sql, tuple([841, 20, 1, 1, 98109]))
mydb.commit()'''