'''
import pandas as pd

# 1: selecting random rows of your dataset
d = pd.DataFrame('ciao.csv')
d1 = d.sample(n=100)

# 2: selecting every k-th (in the example, k=10) row stopping at row 1000
care
f = open('ciao.csv', "r")
k = 10

dataset = []
i = 0
stop = 10000
for row in r.readlines():
	if i%k == 0:
		dataset += [row]
	i += 1
	if i >= stop:
		break
'''



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

from queries import *
# Write <dataset> to a new file 'reduced_dataset.csv'

def load_data(dataset_fname: str):
	print("I loaded the dataset and built the database!\n")
	# dump the database to a file
	pass 


def query_pitStopLewis():
	# yearselected = int(input('select year: ')) ---> non necessario se anno è sempre 2021
	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
	    From PitStops as p, Drivers as d, Races as r
	    Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
	    Group by d.name, d.surname, d.number, r.raceYear
	    Having d.name = 'Lewis'  and d.surname = 'Hamilton'
	    ''')
	result1 = mycursor.fetchall()

	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
			    From PitStops as p, Drivers as d, Races as r
			    Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
			    Group by d.name, d.surname, d.number, r.raceYear
			    Having d.name = 'Max'  and d.surname = 'Verstappen'
			    ''')
	result2 = mycursor.fetchall()
	for row1 in result1:
		for row2 in result2:
			print(f"\n====\n Result of your query: \n {row1}\n{row2}\n ====")


def query_members():
	print(f"<result of query_2>")


def query_rentals():
	print(f"<result of query_3>")





# MAIN
if __name__ == "__main__":
	print("Welcome to our project!\n")
	load_data("mydataset.txt")

	valid_choices = ['query_pitStopLewis','query_members']

	while True:

		choice = input('''\n\nChoose a query to execute by typing 'query_pitStopLewis','query_members',\n
'query_pitStopLewis' -> Total number of pitstops in 2021 season for Max Verstappen and Lewis Hamilton
'members' -> Get all the members
'rentals' -> Get all the rentals
 > ''')

		if choice not in valid_choices:
			print(f"Your choice '{choice}' is not valid. Please retry")
			continue

		if choice == "quit":
			break

		print(f"\nYou chose to execute query {choice}")
		if choice == 'query_pitStopLewis':
			query_pitStopLewis()
		elif choice == 'members':
			query_members()
		elif choice == 'rentals':
			query_rentals()

		else:
			raise Exception("We should never get here!")


	print("\nGoodbye!\n")
