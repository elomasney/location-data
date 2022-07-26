import sqlite3
import pandas

# Connect sqlite database
connection = sqlite3.connect('location.db')

curs = connection.cursor()

curs.execute("create table if not exists locationMap" + "(locationId text primary key, locationName text)")
curs.execute(
    "create table if not exists locationData" + "(locationId text, time datetime, AtomosphericPressure real, WindDirection real, WindSpeed real, Gust real, constraint fk_locationMap foreign key(locationId) references locationMap(locationId))")

locationMap = pandas.read_csv('locationMap.csv')
locationData = pandas.read_csv('locationData.csv', low_memory=False)

locationMap.to_sql('locationMap', connection, if_exists='replace', index=False)
locationData.to_sql('locationData', connection, if_exists='replace', index=False)

# curs.execute('select * from locationMap')
curs.execute('SELECT * FROM locationData join locationMap on locationMap.locationId=locationData.locationId order by locationData.locationId')

places = curs.fetchall()
data = curs.fetchall()

for row in places:
    print(row)

connection.close()
