import mysql.connector
from datetime import datetime

def createTable():
  mycursor.execute("CREATE TABLE IF NOT EXISTS Hours ("
                   "HourTimestamp INTEGER PRIMARY KEY"
                   ");")
  mydb.commit()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="GuetesPasswort123",
  database="basel"
)
mycursor = mydb.cursor()
createTable()

start = 946684800 #timestamp 1.1.2000. 00:00
hourInSeconds = 3600
counter = 0
dateNow = int(datetime.now().timestamp())


sql = "INSERT INTO Hours (HourTimestamp) VALUE (%s)"
mycursor = mydb.cursor()
while True:
    hourTimeStamp = start + counter * hourInSeconds
    mycursor.execute(sql, (hourTimeStamp,))
    mydb.commit()
    counter = counter + 1
    if counter % 1000 == 0:
        print(counter, "entries")
    if hourTimeStamp+hourInSeconds > dateNow:
        break