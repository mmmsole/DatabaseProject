
import mysql.connector

identifier = input('Who are you?\nMa, De or Da?\n')
if identifier == 'Ma':
    pw = 'Tazzadargento_90'
elif identifier == 'Da':
    pw = 'ciaociao'
elif identifier == 'De':
    pw = '#MySQLDemi2022'

db_name = 'F1_db'

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password= pw,
  database= db_name
)

mycursor = mydb.cursor()

def load_data(dataset_fname: str):
	print("I loaded the dataset and built the database!\n")
	pass 


def Query1():
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

	print(f"\n====\n Result of your query: \n{result1}\n{result2}\n ====")


def Query2():
	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
		From Results as res, Races as r, Drivers as d
		Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
		Group by d.name
	    Having d.name = 'Lewis'  and d.surname = 'Hamilton'
	    ''')
	result1 = mycursor.fetchall()

	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
		From Results as res, Races as r, Drivers as d
		Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
		Group by d.name
	    Having d.name = 'Max'  and d.surname = 'Verstappen'
			    ''')
	result2 = mycursor.fetchall()

	print(f"\n====\n Result of your query: \n{result1}\n{result2}\n ====")

def Query3():
	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, r.raceName as GrandPrix, count(p.stopNumber) as NumPitStops
		From PitStops as p, Races as r, Drivers as d
		Where p.raceId = r.raceId and p.driverId = d.driverId and r.raceYear = 2021
		Group by r.raceName
		Having d.name = 'Max' and d.surname = 'Verstappen'
				''')
	result1 = mycursor.fetchall()

	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, r.raceName as GrandPrix, count(p.stopNumber) as NumPitStops
		From PitStops as p, Races as r, Drivers as d
		Where p.raceId = r.raceId and p.driverId = d.driverId and r.raceYear = 2021
		Group by r.raceName
		Having d.name = 'Lewis' and d.surname = 'Hamilton'
			    ''')
	result2 = mycursor.fetchall()
	print(f"\n====\n Result of your query: \n{result1}\n{result2}\n ====")




def Query4():
	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
		From PitStops as p, Drivers as d, Races as r
		Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
		Group by d.name
		Having count(*) < (
			Select x.NumPitStops
			From (
				Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
				From PitStops as p, Drivers as d, Races as r
				Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
				Group by d.name
				Having d.name = 'Max'  and d.surname = 'Verstappen'
			) as x
				)''')

	result = mycursor.fetchall()

	print(f"\n====\n Result of your query: \n{result}\n ====")



def Query5():
	mycursor.execute('''Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
			From Results as res, Races as r, Drivers as d
			Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
			Group by d.name
			Having count(res.fastLap) > (
				Select x.NumFastestLap
				From (
					Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
					From Results as res, Races as r, Drivers as d
					Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
					Group by d.name
					Having d.name = 'Max'  and d.surname = 'Verstappen'
				) as x
					)''')

	result = mycursor.fetchall()

	print(f"\n====\n Result of your query: \n{result}\n ====")


def Query6():
	mycursor.execute('''Select  d.name, d.surname, d.number, sum(res.points) as Standings
		From Drivers as d, Results as res, Races as r
		Where d.driverId = res.driverId and r.raceId = res.raceId and r.raceYear = 2021
		Group by d.name, d.surname
		Order by Standings DESC
		''')

	result = mycursor.fetchall()

	print(f"\n====\n Result of your query: \n{result}\n ====")







# MAIN
if __name__ == "__main__":
	print("Welcome to our project!\n")
	load_data("mydataset.txt")

	valid_choices = ['Query1','Query2','Query3','Query4','Query5','Query6']

	while True:

		choice = input('''\n\nChoose a query to execute by typing 'Query1','Query2 'Query3','Query5','Query6 \n
'Query1' -> Total number of pitstops in 2021 season for Max Verstappen and Lewis Hamilton
'Query2' -> Number of fastest laps for Max Verstappen and Lewis Hamilton
'Query3' -> Number of pitstops per race for Max Verstappen and Lewis Hamilton in 2021
'Query4' -> All drivers who did less pitstops than the 2021 World Champion (Max Verstappen)
'Query5' -> All drivers who did more fastest laps than the 2021 World Champion (Max Verstappen)
'Query6'  -> Drivers' standings for a given season
 > ''')

		if choice == "quit":
			break

		if choice not in valid_choices:
			print(f"Your choice '{choice}' is not valid. Please retry")
			continue

		print(f"\nYou chose to execute query {choice}")
		if choice == 'Query1':
			Query1()
		elif choice == 'Query2':
			Query2()
		elif choice == 'Query3':
			Query3()
		elif choice == 'Query4':
			Query4()
		elif choice == 'Query5':
			Query5()
		elif choice == 'Query6':
			Query6()

		else:
			raise Exception("We should never get here!")


	print("\nGoodbye!\n")
