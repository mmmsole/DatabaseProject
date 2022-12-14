import mysql.connector
from mysql.connector import errorcode

db_name = 'F1_db'

identifier = input('Who are you?\nMa, De or Da?\n')
print(identifier)
if identifier == 'Ma':
    pw = 'Tazzadargento_90'
elif identifier == 'Da':
    pw = 'ciaociao'
elif identifier == 'De':
    pw = '#MySQLDemi2022'

try:
    mydb = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password=pw,
                                   auth_plugin='mysql_native_password')# you can add the auth_plugin here too (ref line 26)

    if mydb.is_connected():
        mycursor = mydb.cursor()
        mycursor.execute('SHOW DATABASES')
        result = mycursor.fetchall()
        print(result)
        for x in result:
            if db_name == x[0]:
                mycursor.execute('DROP DATABASE ' + db_name)  # delete old database
                mydb.commit()  # make the changes official
                print("The database already exists! The old database has been deleted!)")

        mycursor.execute("CREATE DATABASE " + db_name)
        print("Database is created")
except errorcode as e:
    print("Error while connecting to MySQL", e)

mycursor.execute("USE " + db_name)
print("Database changed")