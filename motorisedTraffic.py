import codecs
import csv
import time
import mysql.connector
from datetime import datetime, timezone
import pandas as pd


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
    mycursor.execute("CREATE TABLE IF NOT EXISTS motorisedTraffic ("
                     "CityID INTEGER PRIMARY KEY REFERENCES BaselData(CityID),"
                     "SiteCode VARCHAR(30),"
                     "SiteName VARCHAR(60),"
                     "DirectionName VARCHAR(50),"
                     "LaneCode INTEGER,"
                     "LaneName VARCHAR(30),"
                     "Date VARCHAR(30),"
                     "TimeFrom VARCHAR(30),"
                     "TimeTo VARCHAR(30),"
                     "ValuesApproved INTEGER,"
                     "ValuesEdited INTEGER,"
                     "TrafficType VARCHAR(30),"
                     "Total INTEGER,"
                     "MR INTEGER,"
                     "PW INTEGER,"
                     "PWplus INTEGER,"
                     "Lief INTEGER,"
                     "Liefplus INTEGER,"
                     "LiefPlusAufl INTEGER,"
                     "LW  INTEGER,"
                     "LWplus INTEGER,"
                     "Sattelzug INTEGER,"
                     "Bus INTEGER,"
                     "andere INTEGER"
                     ");")
    mydb.commit()


def dropMotTraf():
    mycursor.execute("DROP TABLE IF EXISTS motorisedTraffic;")
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
dropMotTraf()
# dropBasData()
createTable1()
createTable2()

mycursor.execute("SELECT COUNT(*) FROM BaselData")

basId = mycursor.fetchone()[0]

print("Size of BaselID before this table is implemented is", basId)

sqlBS = "INSERT INTO BaselData (CityID, TimeStamp, HourTimestamp) " \
        "VALUES (%s,%s,%s)"
sqlMot = "INSERT INTO motorisedTraffic (CityID, SiteCode, SiteName, DirectionName, LaneCode, LaneName, Date, TimeFrom," \
         "TimeTo, ValuesApproved, ValuesEdited, TrafficType, Total, MR, PW, PWplus, Lief, Liefplus, LiefPlusAufl, " \
         "LW, LWplus, Sattelzug, Bus, andere) " \
         "VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

time0 = time.time()
totalRows = 5072496

with codecs.open(r'C:\Users\Tobit\DatabasesProject\src\Verkehrszähldaten motorisierter Individualverkehr.csv', 'rU',
                 encoding='unicode_escape') as f:
    f.readline()
    reader = csv.reader(fix_nulls(f), delimiter=";", quotechar="\"")
    print("starting the loop.")
    for i, line in enumerate(reader):
        if i < 175000:
            continue
        tsString = f"{str(line[5])} {str(line[6])}"
        ts = int(datetime.strptime(tsString, "%d.%m.%Y %H:%M").timestamp())
        tsHour = int(
            datetime.utcfromtimestamp(ts).replace(minute=0, second=0, microsecond=0).replace(tzinfo=timezone.utc).
            timestamp())
        if ts != tsHour:
            print("ts:", ts)
            print("tsh:", tsHour)

        if line[3] == '': #there is missing data
            laneCode = -1
        else:
            laneCode = line[3]

        mycursor.execute(sqlBS, (basId, ts, tsHour))
        mycursor.execute(sqlMot, (
            basId, str(line[0]), str(line[1]), str(line[2]), laneCode, str(line[4]), str(line[5]), str(line[6]),
            str(line[7]), int(line[8]), int(line[9]), str(line[10]), int(line[11]), int(line[12]), int(line[13]),
            int(line[14]), int(line[15]), int(line[16]), int(line[17]), int(line[18]), int(line[19]), int(line[20]),
            int(line[21]), int(line[22])))
        mydb.commit()
        basId = basId + 1

        if i % 25000 == 0:
            timeRun = time.time() - time0
            percentDone = i / totalRows * 100
            if percentDone != 0:
                print(percentDone, "percent done,", i, "rows inserted, running since", timeRun // 1,
                      "seconds or", timeRun // 60, "minutes. The program will run for around",
                      timeRun * 100 / percentDone // 60, "more minutes.")
            else:
                print(percentDone, "procent done,", i, "rows inserted, running since", timeRun // 1,
                      "seconds or", timeRun // 60, "minutes. The program will run for a long time.")
    print("Hurray, table is integrated! In total,", i, "rows have been inserted.")
