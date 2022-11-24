import pandas as pd

circuits = pd.read_csv('archive/circuits.csv')
drivers_1 = pd.read_csv('archive/drivers.csv')
lap_times_1 = pd.read_csv('archive/lap_times.csv')
pit_stops_1 = pd.read_csv('archive/pit_stops.csv')
races_1 = pd.read_csv('archive/races.csv')
results_1 = pd.read_csv('archive/results.csv')

# FROM RACES GET YEAR < 2002
races = races_1.loc[races_1["year"] > 2002]
races = races.iloc[:,[0,1,2,3,4,5,6]]

# GET LAP TIMES WITH RACE ID
lap_times = lap_times_1[lap_times_1['raceId'].isin(races['raceId'])]

# GET PIT STOP WITH RACE ID
pit_stops = pit_stops_1[pit_stops_1['raceId'].isin(races['raceId'])]  

# GET RESULT WITH RACE ID
results = results_1[results_1['raceId'].isin(races['raceId'])]

# GET DRIVERS WITH LAP TIMES
drivers = drivers_1[drivers_1['driverId'].isin(lap_times['driverId'])]











