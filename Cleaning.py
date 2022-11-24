import pandas as pd

circuits = pd.read_csv('archive/circuits.csv')
drivers = pd.read_csv('archive/drivers.csv')
lap_times = pd.read_csv('archive/lap_times.csv')
pit_stops = pd.read_csv('archive/pit_stops.csv')
races_1 = pd.read_csv('archive/races.csv')
results = pd.read_csv('archive/results.csv')

# FROM RACES GET YEAR < 2002
races = races_1.loc[races_1["year"] > 2002]
# GET LAP TIMES WITH RACE ID ABOVE






