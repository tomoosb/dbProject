import mysql.connector
from datetime import datetime
import codecs
import csv
import time

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
    mycursor.execute("CREATE TABLE IF NOT EXISTS Hotelrooms ("
                       "CityID INTEGER PRIMARY KEY REFERENCES BaselData(CityID),"
                       "Date VARCHAR(30),"
                       "Cathegorie VARCHAR(20),"
                       "NrOfNights INTEGER,"
                       "NrFreeRooms INTEGER,"
                       "NrOccupiedRooms INTEGER,"
                       "Year INTEGER,"
                       "Month INTEGER,"
                       "Day INTEGER"
                       ");")
    mydb.commit()

def dropHotels():
    mycursor.execute("DROP TABLE IF EXISTS Hotelrooms;")
    mydb.commit()

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="GuetesPasswort123",
  database="basel"
)
mycursor = mydb.cursor()

dropHotels()
createTable1()
createTable2()

mycursor.execute("SELECT COUNT(*) FROM BaselData")

basId = mycursor.fetchone()[0]

print("Size of BaselID before this table is implemented is ", basId)

sqlBS = "INSERT INTO BaselData (CityID, TimeStamp, HourTimestamp) " \
       "VALUES (%s,%s,%s)"
sqlHotel = "INSERT INTO Hotelrooms (CityID, Date, Cathegorie, NrOfNights,NrFreeRooms,NrOccupiedRooms, Year, Month, Day) " \
       "VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

time0 = time.time()


with codecs.open(r'C:\Users\Tobit\DatabasesProject\src\Tägliche Logiernächte, verfügbare und belegte Zimmer.csv', 'rU', encoding='utf-8') as f:
  f.readline()
  reader = csv.reader(fix_nulls(f), delimiter=";", quotechar="\"")
  print("starting the loop.")
  for i, line in enumerate(reader):
      mycursor.execute(sqlHotel, (basId, str(line[0]),str(line[1]),int(line[2]),int(line[3]),int(line[4]),int(line[5]),int(line[6]),int(line[7])))
      #since these are daily datapoints, we decided to put the timestamp at 00:00 of each day
      ts = int(datetime.strptime(str(line[0]), "%Y-%m-%d").replace(hour=0, minute=0, second=0).timestamp())
      tsHour = ts
      mycursor.execute(sqlBS, (basId, ts, tsHour))
      mydb.commit()
      basId = basId + 1
      if i % 100 == 0:
          print(i / 12904 * 100, "percent done,", i, "rows inserted, running since", (time.time() - time0),
                "secounds or", (time.time() - time0) / 60, "minutes.")
  print("Hurray, table is integrated! In total,", i, "rows have been inserted.")