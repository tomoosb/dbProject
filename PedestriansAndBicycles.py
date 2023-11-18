import time
import mysql.connector
from datetime import datetime, timezone
import pandas as pd


def createTable1():
    mycursor.execute("CREATE TABLE IF NOT EXISTS BaselData ("
                     "CityID INTEGER PRIMARY KEY,"
                     "TimeStamp INTEGER,"
                     "HourTimeStamp INTEGER REFERENCES Hours(HourTimestamp)"
                     ");")
    mydb.commit()


def createTable2():
    mycursor.execute("CREATE TABLE IF NOT EXISTS PedAndBic ("
                     "CityID INTEGER PRIMARY KEY REFERENCES BaselData(CityID),"
                     "SiteCode VARCHAR(10),"
                     "SiteName VARCHAR(50),"
                     "DirectionName VARCHAR(50),"
                     "LaneCode INTEGER,"
                     "LaneName VARCHAR(30),"
                     "Date VARCHAR(30),"
                     "TimeFrom VARCHAR(10),"
                     "TimeTo VARCHAR(10),"
                     "ValuesApproved INTEGER,"
                     "ValuesEdited INTEGER,"
                     "TrafficType VARCHAR(30),"
                     "Total INTEGER"
                     ");")
    mydb.commit()


def dropPedAndBic():
    mycursor.execute("DROP TABLE IF EXISTS PedAndBic;")
    mydb.commit()


def dropBasData():
    mycursor.execute("DROP TABLE IF EXISTS BaselData;")
    mydb.commit()


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="GuetesPasswort123",
    database="basel"
)
mycursor = mydb.cursor()
timeStart = time.time()
dropPedAndBic()
dropBasData()
createTable1()
createTable2()

mycursor.execute("SELECT COUNT(*) FROM BaselData")

basId = mycursor.fetchone()[0]

sqlBS = "INSERT INTO BaselData (CityID, TimeStamp, HourTimestamp) " \
        "VALUES (%s,%s,%s)"
sqlPed = "INSERT INTO PedAndBic (CityID, SiteCode, SiteName, DirectionName, LaneCode, LaneName, Date, TimeFrom, TimeTo, ValuesApproved, ValuesEdited, TrafficType, Total) " \
         "VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

time0 = time.time()
print("loading csv into pandas")
PedBic = pd.read_csv(r'C:\Users\Tobit\DatabasesProject\src\Verkehrszähldaten Velos und Fussgänger.csv',
                     encoding='unicode_escape')
df = pd.DataFrame(PedBic)
print("loaded in", (time.time() - time0) // 1, "seconds, now splitting the dataframe.")

time99 = time.time()
df[['SiteCode', "SiteName", "DirectionName", "LaneCode", "LaneName", "Date", "TimeFrom", "TimeTo", "ValuesApproved",
    "ValuesEdited", "TrafficType", "Total"]] = df[
    'SiteCode;SiteName;DirectionName;LaneCode;LaneName;Date;TimeFrom;TimeTo;ValuesApproved;ValuesEdited;TrafficType;Total'].str.split(
    ';', expand=True)
df = df.drop(columns=[
    'SiteCode;SiteName;DirectionName;LaneCode;LaneName;Date;TimeFrom;TimeTo;ValuesApproved;ValuesEdited;TrafficType;Total'])
dfNoNan = df.fillna(0)



siteCode = dfNoNan['SiteCode']
siteName = dfNoNan['SiteName']
DirName = dfNoNan["DirectionName"]
laneCode = dfNoNan["LaneCode"].replace('', '-1')
laneName = dfNoNan["LaneName"]
datum = dfNoNan["Date"]
timeFrom = dfNoNan["TimeFrom"]
timeTo = dfNoNan["TimeTo"]
valApproved = dfNoNan["ValuesApproved"].replace('', '-1')
valEdited = dfNoNan["ValuesEdited"].replace('', '-1')
traffType = dfNoNan["TrafficType"]
total = dfNoNan["Total"].replace('', '-1')
del df, dfNoNan

print("splitted in", (time.time() - time99) // 1, "seconds. Starting with the creation of the batches now.")
time1 = time.time()
totalRows = len(total)  # 7916400

batch_size = 1000  # Adjust the batch size as needed

# Sample code for bulk insertion for pedandbic table
data_ped = [(basId + i, str(siteCode[i]), str(siteName[i]), str(DirName[i]), int(laneCode[i]), str(laneName[i]),
             str(datum[i]), str(timeFrom[i]), str(timeTo[i]), int(valApproved[i]), int(valEdited[i]),
             str(traffType[i]), int(total[i])) for i in range(totalRows)]

# Sample code for bulk insertion for BaselData table
data_bas = [(basId + i, int(datetime.strptime(f"{datum[i]} {timeFrom[i]}", "%d.%m.%Y %H:%M").timestamp()),
             int(datetime.strptime(f"{datum[i]} {timeFrom[i]}", "%d.%m.%Y %H:%M").replace(minute=0, second=0,
                                                                                          microsecond=0).replace(
                 tzinfo=timezone.utc).timestamp())) for i in range(totalRows)]

del siteCode, siteName, DirName, laneCode, laneName, datum, timeFrom, timeTo, valApproved, valEdited, traffType, total

print("batches made ready in", (time.time() - time1) // 1, "seconds or", (time.time() - time1) // 60,
      "minutes. Starting integration now.")

time1 = time.time()
counter = 0
for i in range(0, totalRows, batch_size):
    batch_data_ped = data_ped[i:i + batch_size]
    mycursor.executemany(sqlPed, batch_data_ped)

    # Adjust basId for the next batch if needed
    basId += batch_size

    mydb.commit()
    timeRun = time.time() - time1
    percentDone = i / totalRows * 100

    batch_data_ped = data_ped[i:i + batch_size]
    mycursor.executemany(sqlPed, batch_data_ped)

    mydb.commit()

    # Adjust basId for the next batch if needed
    basId += batch_size

    timeRun = time.time() - time1
    percentDone = i / totalRows * 100
    if counter % 25 == 0:
        if percentDone != 0:
            print(percentDone, "percent done,", i, "rows inserted, running since", timeRun // 1,
                  "seconds or", timeRun // 60, "minutes. The program will run for around",
                  timeRun * 100 / percentDone // 60, "more minutes.")
    counter += 1
print("Hurray, table is integrated! In total,", totalRows, "rows have been inserted. The program took in total",timeStart // 1,
                  "seconds or", timeStart // 60, "minutes.")
