import pandas as pd

circuits_1 = pd.read_csv('../archive/circuitsRaw.csv')
drivers_1 = pd.read_csv('../archive/driversRaw.csv')
races_1 = pd.read_csv('../archive/racesRaw.csv')
results_1 = pd.read_csv('../archive/resultsRaw.csv')
lap_times_1 = pd.read_csv('../archive/lap_timesRaw.csv')
pit_stops_1 = pd.read_csv('../archive/pit_stopsRaw.csv')

# GET ALL CIRCUITS
circuits = circuits_1.iloc[:, 0:5]
#print(circuits.info())

# FROM RACES GET YEAR < 2002
races = races_1.loc[races_1["year"] > 2002]
races = races.iloc[:,0:7]
#to get some info about the table
#print(races.info())
#print(races_1.shape)
#print(races.shape)

# GET LAP TIMES OF OUR RACES
lap_times = lap_times_1[lap_times_1['raceId'].isin(races['raceId'])]
lap_times = lap_times.iloc[:,[0,1,2,3,4,5]]
#to get some info about the table
#print(lap_times.info())
#print(lap_times_1.shape)
#print(lap_times.shape)

# GET PIT STOPS IN OUR LAP TIMES
pit_stops = pit_stops_1[pit_stops_1['raceId'].isin(lap_times['raceId'])]
pit_stops = pit_stops.iloc[:,0:6]
for i in pit_stops['duration']:
    if len(i) > 6:
        pit_stops['duration'] = pit_stops['duration'].replace(i,r'\N')
#to get some info about the table
#print(pit_stops.info())
#print(pit_stops_1.shape)
#print(pit_stops.shape)

# GET RESULTS OF OUR RACES
results = results_1[results_1['raceId'].isin(races['raceId'])]
results = results.iloc[:,[0,1,2,5,6,9,13]]
#to get some info about the table
#print(results.info())
#print(results_1.shape)
#print(results.shape)

# GET DRIVERS THAT DID OUR LAP TIMES
drivers = drivers_1[drivers_1['driverId'].isin(lap_times['driverId'])]
drivers = drivers.iloc[:,:-1]
#drivers = drivers.replace(r'\N','NULL')
#to get some info about the table
#print(drivers.info())
#print(drivers_1.shape)
#print(drivers.shape)







