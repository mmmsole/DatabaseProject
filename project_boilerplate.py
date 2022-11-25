'''
import pandas as pd

# 1: selecting random rows of your dataset
d = pd.DataFrame('ciao.csv')
d1 = d.sample(n=100)

# 2: selecting every k-th (in the example, k=10) row stopping at row 1000

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
from queries import *
# Write <dataset> to a new file 'reduced_dataset.csv'

def load_data(dataset_fname: str):
	print("I loaded the dataset and built the database!\n")
	# dump the database to a file
	pass 


def query_pitStopLewis():
	yearselected = int(input('select year: '))
	query1
	result = mycursor.fetchall()
#	print(f"\n====\n<result of query_1: {query1}>\n====")


def query_members():
	print(f"<result of query_2>")


def query_rentals():
	print(f"<result of query_3>")





# MAIN
if __name__ == "__main__":
	print("Welcome to our project!\n")
	load_data("mydataset.txt")

	valid_choices = ['cars', 'members', 'rentals', 'quit']

	while True:

		choice = input('''\n\nChoose a query to execute by typing 'cars', 'members', or 'rentals', or type 'quit' to quit.\n
'cars' -> Get all the cars with engineCC greater than a given value
'members' -> Get all the members
'rentals' -> Get all the rentals
 > ''')

		if choice not in valid_choices:
			print(f"Your choice '{choice}' is not valid. Please retry")
			continue

		if choice == "quit":
			break

		print(f"\nYou chose to execute query {choice}")
		if choice == 'cars':
			query_pitStopLewis()
		elif choice == 'members':
			query_members()
		elif choice == 'rentals':
			query_rentals()

		else:
			raise Exception("We should never get here!")


	print("\nGoodbye!\n")
