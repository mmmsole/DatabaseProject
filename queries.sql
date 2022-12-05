-- GROUP B (FORMULA 1) QUERIES

-- QUERY 1

Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(p.stopNumber) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.driverId
        Having d.name = 'Max'  and d.surname = 'Verstappen'

Select d.name as Name, d.surname as Surname, d.number as DriverNumber, r.raceYear as Year, count(p.stopNumber) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.driverId
        Having d.name = 'Lewis'  and d.surname = 'Hamilton'

-- QUERY 2

Select d.name as Name, d.surname as Surname, r.raceYear as Year, r.raceName as GrandPrix, c.name as CircuitName, count(*) as NumPitStops
        From PitStops as p, Races as r, Drivers as d, Circuits as c
        Where p.raceId = r.raceId and p.driverId = d.driverId and r.circuitId = c.circuitId and r.raceYear = 2021
        Group by r.raceName, d.driverId, CircuitName
        Having d.name = 'Max' and d.surname = 'Verstappen'

Select d.name as Name, d.surname as Surname, r.raceYear as Year, r.raceName as GrandPrix, c.name as CircuitName, count(*) as NumPitStops
        From PitStops as p, Races as r, Drivers as d, Circuits as c
        Where p.raceId = r.raceId and p.driverId = d.driverId and r.circuitId = c.circuitId and r.raceYear = 2021
        Group by r.raceName, d.driverId, CircuitName
        Having d.name = 'Lewis' and d.surname = 'Hamilton'

-- QUERY 3

Select d.name as Name, d.surname as Surname, r.raceYear as Year, count(p.stopNumber) as NumPitStops
        From PitStops as p, Drivers as d, Races as r
        Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
        Group by d.driverId
        Having count(p.stopNumber) < (
            Select x.NumPitStops
            From (
                Select d.name as Name, d.surname as Surname, r.raceYear as Year, count(*) as NumPitStops
                From PitStops as p, Drivers as d, Races as r
                Where p.driverId = d.driverId and p.raceId = r.raceId and r.raceYear = 2021
                Group by d.name, d.surname
                Having d.name = 'Max'  and d.surname = 'Verstappen'
            ) as x
        )
        ORDER BY NumPitStops DESC

-- QUERY 4

Select  d.name, d.surname, sum(res.points) as Standings
        From Drivers as d, Results as res, Races as r
        Where d.driverId = res.driverId and r.raceId = res.raceId and r.raceYear = {year}
        Group by d.name, d.surname
        Order by Standings DESC

-- QUERY 5

Select d.name as DriverName, d.surname as DriverSurname, SUM(r.points) as TotalPoints
        From Drivers as d, Results as r
        Where d.driverId = r.driverId and d.nationality = '{nat}'
        Group by d.driverId
        Order by TotalPoints DESC

-- QUERY 6

Select name as Name, surname as surname
        From Drivers
        Where driverId NOT IN (
            Select distinct driverId
            From Results
            Where finalPos = 1
        )

-- QUERY 7

Select name, surname
        From Drivers
        Where driverId IN (
            Select driverId
            From Results
            Group by driverId
            Having SUM(points) = 0
        )

-- QUERY 8

Select r.raceName, r.raceYear, AVG(p.stopNumber) as AvgPitStops
        From Races as r, PitStops as p
        where p.raceId = r.raceId and r.raceYear = {year}
        Group by r.raceId

-- QUERY 9

Select r.raceName as GrandPrix, r.raceYear as RaceYear, r.raceDate as RaceDate, d.name as WinnerName, d.surname as WinnerSurname
    From Drivers as d, Results as res, Races as r
    Where d.driverId = res.driverId and res.raceId = r.raceId and r.raceYear = {year} and res.finalPos = 1

-- QUERY 10

Select r.raceName as GrandPrix, r.raceDate as RaceDate, d.name as DriverName, d.surname as DriverSurname, res.gridPos as GridPosition, res.finalPos as FinalPosition
    From Drivers as d, Results as res, Races as r
    Where d.driverId = res.driverId and r.raceId = res.raceId and r.raceYear = {year} and res.gridPos > 10 and (res.finalPos >= 1 and res.finalPos <= 3)






