import time
import mysql.connector
import codecs
import csv
from datetime import datetime, timezone

def fix_nulls(s):
  for line in s:
    yield line.replace('\0', ' ')

def createTable1():
  mycursor.execute("CREATE TABLE IF NOT EXISTS BaselData ("
                   "CityID INTEGER PRIMARY KEY,"
                   "TimeStamp INTEGER,"
                   "HourTimeStamp INTEGER REFERENCES Hours(HourTimestamp)"
                   ");")
  mydb.commit()

def createTable2():
    mycursor.execute("CREATE TABLE IF NOT EXISTS Carparks ("
                     "CityID INTEGER PRIMARY KEY REFERENCES BaselData(CityID),"
                     "NrFree INTEGER,"
                     "TotalSpace INTEGER,"
                     "ProcFull VARCHAR(30),"
                     "NameLong VARCHAR(60),"
                     "NameShort  VARCHAR(30),"
                     "Title VARCHAR(30),"
                     "Name VARCHAR(30),"
                     "Adress VARCHAR(30),"
                     "Link VARCHAR(100),"
                     "Geopoint VARCHAR(30),"
                     "Description VARCHAR(30)"
                     ");")
    mydb.commit()

def dropCarparks():
    mycursor.execute("DROP TABLE IF EXISTS Carparks;")
    mydb.commit()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="GuetesPasswort123",
  database="basel"
)
mycursor = mydb.cursor()
dropCarparks()
createTable1()
createTable2()

mycursor.execute("SELECT COUNT(*) FROM BaselData")

basId = mycursor.fetchone()[0]

print("Size of BaselID before this table is implemented is ", basId)


sqlBS = "INSERT INTO BaselData (CityID, TimeStamp, HourTimestamp) " \
       "VALUES (%s,%s,%s)"
sqlCar = "INSERT INTO Carparks (CityID,NrFree ,TotalSpace,ProcFull,NameLong,NameShort,Title,Name,Adress,Link ,Geopoint,Description) " \
       "VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

time0 = time.time()
with codecs.open(r'C:\Users\Tobit\DatabasesProject\src\Zeitreihe der Belegung öffentlicher Parkhäuser Basel.csv', 'rU', encoding='utf-8') as f:
  f.readline()
  reader = csv.reader(fix_nulls(f), delimiter=";", quotechar="\"")
  print("starting the loop.")
  for i, line in enumerate(reader):
      ts = int(datetime.fromisoformat(line[0]).replace(tzinfo=timezone.utc).timestamp())
      tsHour = int(datetime.utcfromtimestamp(ts).replace(minute=0, second=0, microsecond=0).replace(tzinfo=timezone.utc).timestamp())

      mycursor.execute(sqlBS, (basId, ts, tsHour))
      mycursor.execute(sqlCar, (basId, str(line[1]), int(line[2]), str(line[3]), str(line[4]), str(line[5]), str(line[6]), str(line[7]),
      str(line[8]), str(line[9]), str(line[10]), str(line[11])))
      mydb.commit()
      basId = basId + 1

      if i % 10000 == 0:
          print(i/640000*100, "percent done,", i, "rows inserted, running since", (time.time()-time0),"secounds or", (time.time()-time0)/60, "minutes.")

  print("Hurray, table is integrated! In total,", i, "rows have been inserted.")