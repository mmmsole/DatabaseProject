# read the dataset from workspace

import pandas as pd
import mysql.connector
import pandas as pd
from mysql.connector import errorcode
from Cleaning import *
from connectionPythonMySql import *

print(circuits.info())

'''mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="ciaociao",
  database= db_name
)'''

mycursor = mydb.cursor()
# Insert DataFrame records one by one.

for i,row in circuits.iterrows():
            sql = "INSERT INTO db_name.CIRCUITS VALUES (%s,%s,%s,%s,%s)"
            mycursor.execute(sql, tuple([row['circuitId'], row['circuitRef'], row['name'], row['location'], row['country']]))
            #print("Record inserted")
            mydb.commit()


