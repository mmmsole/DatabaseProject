import mysql.connector

db_name = 'F1_db'

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="ciaociao",
  database= db_name
)

mycursor = mydb.cursor()

#mycursor.execute("SELECT * FROM CIRCUITS")

mycursor.execute('''SELECT count(*), nationality FROM DRIVERS
                group by nationality ''')

result = mycursor.fetchall()
for row in result:
  print(row)