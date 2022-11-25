import mysql.connector

db_name = 'F1_db'

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Tazzadargento_90",
  database= db_name
)

mycursor = mydb.cursor()

#mycursor.execute("SELECT * FROM CIRCUITS")

#mycursor.execute('''SELECT count(*), nationality FROM DRIVERS
#        group by nationality''')

mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
    From PitStops as p, Drivers as d, Races as r
    Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
    Group by d.name, d.surname, d.number, r.raceYear
    Having d.name = 'Lewis'  and d.surname = 'Hamilton'
    ''')


result = mycursor.fetchall()
for row in result:
  print(row)