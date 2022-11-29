import mysql.connector

pw = ""
user = "Stranger"

identifier = input('Who are you?\nMa, De or Da?\nIf you are someone else, enter.\n> ')
print(identifier)
if identifier == 'Ma':
    pw = 'Tazzadargento_90'
    user = 'Mariasole'
elif identifier == 'Da':
    pw = 'ciaociao'
    user = 'Davide'
elif identifier == 'De':
    pw = '#MySQLDemi2022'
    user = 'Demetrio'
else:
    user = input('What is your name?\n> ')
    pw = input('Enter your password, please.\n> ')

db_name = 'F1_db'

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=pw,
    database=db_name
)

mycursor = mydb.cursor()


def load_data(dataset_fname: str):
    print("I loaded the dataset and built the database!\n")
    pass


def query1():
    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.name, d.surname, d.number, r.raceYear
        Having d.name = 'Lewis'  and d.surname = 'Hamilton'
        ''')
    result1 = mycursor.fetchall()

    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.name, d.surname, d.number, r.raceYear
        Having d.name = 'Max'  and d.surname = 'Verstappen'
        ''')
    result2 = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result1}\n{result2}\n====\n")


def query2():
    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
        From Results as res, Races as r, Drivers as d
        Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
        Group by d.name, d.surname, d.number, r.raceYear
        Having d.name = 'Lewis'  and d.surname = 'Hamilton'
        ''')
    result1 = mycursor.fetchall()

    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
        From Results as res, Races as r, Drivers as d
        Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
        Group by d.name, d.surname, d.number, r.raceYear
        Having d.name = 'Max'  and d.surname = 'Verstappen'
        ''')
    result2 = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result1}\n{result2}\n====\n")


def query3():
    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, r.raceName as GrandPrix, count(p.stopNumber) as NumPitStops
        From PitStops as p, Races as r, Drivers as d
        Where p.raceId = r.raceId and p.driverId = d.driverId and r.raceYear = 2021
        Group by r.raceName
        Having d.name = 'Max' and d.surname = 'Verstappen'
        ''')
    result1 = mycursor.fetchall()

    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, r.raceName as GrandPrix, count(p.stopNumber) as NumPitStops
        From PitStops as p, Races as r, Drivers as d
        Where p.raceId = r.raceId and p.driverId = d.driverId and r.raceYear = 2021
        Group by r.raceName
        Having d.name = 'Lewis' and d.surname = 'Hamilton'
        ''')
    result2 = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result1}\n{result2}\n====\n")


def query4():
    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
        d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.name
        Having count(*) < (
            Select x.NumPitStops
            From (
                Select d.name as Name, d.surname as Surname, 
                d.number as DriverNumber, r.raceYear as Year, count(*) as NumPitStops
                From PitStops as p, Drivers as d, Races as r
                Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
                Group by d.name
                Having d.name = 'Max'  and d.surname = 'Verstappen'
            ) as x
                )''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


def query5():
    mycursor.execute('''Select d.name as Name, d.surname as Surname, 
            d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
            From Results as res, Races as r, Drivers as d
            Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
            Group by d.name
            Having count(res.fastLap) > (
                Select x.NumFastestLap
                From (
                    Select d.name as Name, d.surname as Surname, 
                    d.number as DriverNumber, r.raceYear as Year, count(res.fastLap) as NumFastestLap
                    From Results as res, Races as r, Drivers as d
                    Where res.raceId = r.raceId and res.driverId = d.driverId and r.raceYear = 2021
                    Group by d.name
                    Having d.name = 'Max'  and d.surname = 'Verstappen'
                ) as x
                    )''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


def query6():
    mycursor.execute('''Select  d.name, d.surname, d.number, sum(res.points) as Standings
        From Drivers as d, Results as res, Races as r
        Where d.driverId = res.driverId and r.raceId = res.raceId and r.raceYear = 2021
        Group by d.name, d.surname
        Order by Standings DESC
        ''')

    result = mycursor.fetchall()

    print(f"\n====\nResult of your query: \n{result}\n====\n")


# MAIN

if __name__ == "__main__":

    print(f"\nWelcome to our project, {user}!\n")

    load_data("mydataset.txt")

    valid_choices = ['query1', 'query2', 'query3', 'query4', 'query5', 'query6']

    while True:

        choice = input('''Choose a query to execute by typing one among the following:\n
'query1' -> Total number of pit stops in 2021 season for Max Verstappen and Lewis Hamilton
'query2' -> Number of fastest laps for Max Verstappen and Lewis Hamilton
'query3' -> Number of pit stops per race for Max Verstappen and Lewis Hamilton in 2021
'query4' -> All drivers who did fewer pit stops than the 2021 World Champion (Max Verstappen)
'query5' -> All drivers who ran more fastest laps than the 2021 World Champion (Max Verstappen)
'query6' -> Drivers' standings for a given season
> ''')

        if choice == "quit":
            break

        if choice not in valid_choices:
            print(f"Your choice '{choice}' is not valid. Please retry")
            continue

        print(f"\nYou chose to execute query {choice}")
        if choice == 'query1':
            query1()
        elif choice == 'query2':
            query2()
        elif choice == 'query3':
            query3()
        elif choice == 'query4':
            query4()
        elif choice == 'query5':
            query5()
        elif choice == 'query6':
            query6()

        else:
            raise Exception("We should never get here!")

    print("\nGoodbye!\n")
