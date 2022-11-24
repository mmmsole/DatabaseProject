# read the dataset from workspace

import pandas as pd
import mysql.connector
import pandas as pd
from mysql.connector import errorcode

circuits = pd.read_csv('archive/circuits.csv')
db_name = 'Formula1_database'

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="ciaociao",
  database= db_name
)

mycursor = mydb.cursor()
# Insert DataFrame records one by one.

for i,row in circuits.iterrows():
            sql = "INSERT INTO db_name.CIRCUITS VALUES (%s,%s,%s,%s,%s)"
            mycursor.execute(sql, tuple([row['circuitId'], row['circuitRef'], row['name_'], row['location'], row['country'], row['lat'], row['lng'], row['alt'], row['url']]))
            #print("Record inserted")
            mydb.commit()


