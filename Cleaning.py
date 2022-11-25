import pandas as pd
import numpy as np

circuits_1 = pd.read_csv('archive/circuits.csv')
drivers_1 = pd.read_csv('archive/drivers.csv')
lap_times_1 = pd.read_csv('archive/lap_times.csv')
pit_stops_1 = pd.read_csv('archive/pit_stops.csv')
races_1 = pd.read_csv('archive/races.csv')
results_1 = pd.read_csv('archive/results.csv')

# FROM RACES GET YEAR < 2002
races = races_1.loc[races_1["year"] > 2002]
races = races.iloc[:,0:7]
#to get some info about the table
#print(races.info())
#print(races_1.shape)
#print(races.shape)


# GET LAP TIMES WITH RACE ID
lap_times = lap_times_1[lap_times_1['raceId'].isin(races['raceId'])]
lap_times = lap_times.iloc[:,[0,1,2,3,5]]

'''for i, row in lap_times.iterrows():
    row['raceId'] = row['raceId'].astype(np.int32)
    row['driverId'] = row['driverId'].astype(np.int32)
    row['lap'] = row['lap'].astype(np.int32)
    row['position'] = row['position'].astype(np.int32)
    row['milliseconds'] = row['milliseconds'].astype(np.int32)
    #print(type(row['milliseconds']))'''

'''for i, row in lap_times.iterrows():
    row['raceId'] = int(row['raceId'])
    row['driverId'] = int(row['driverId'])
    row['lap'] = int(row['lap'])
    row['position'] = int(row['position'])
    row['milliseconds'] = int(row['milliseconds'])'''

'''for i in lap_times:
    pd.to_numeric(lap_times[i], downcast='integer')'''

'''for i in lap_times:
    for j in lap_times[i]:
        lap_times[i] = lap_times[i].replace(j,int(j))'''
#to get some info about the table
#print(lap_times.info())
#print(lap_times_1.shape)
#print(lap_times.shape)

# GET PIT STOP WITH RACE ID
pit_stops = pit_stops_1[pit_stops_1['raceId'].isin(lap_times['raceId'])]
pit_stops = pit_stops.iloc[:,0:6]
for i in pit_stops['duration']:
    if len(i) > 6:
        pit_stops['duration'] = pit_stops['duration'].replace(i,r'\N')

#to get some info about the table
#print(pit_stops.info())
#print(pit_stops_1.shape)
#print(pit_stops.shape)

# GET RESULT WITH RACE ID
results = results_1[results_1['raceId'].isin(races['raceId'])]
results = results.iloc[:,[0,1,2,5,6,9,13]]
#to get some info about the table
#print(results.info())
#print(results_1.shape)
#print(results.shape)

# GET DRIVERS WITH LAP TIMES
drivers = drivers_1[drivers_1['driverId'].isin(lap_times['driverId'])]
drivers = drivers.iloc[:,:-1]
#drivers = drivers.replace(r'\N','NULL')
#to get some info about the table
#print(drivers.info())
#print(drivers_1.shape)
#print(drivers.shape)

circuits = circuits_1.iloc[:, 0:5]
#print(circuits.info())









