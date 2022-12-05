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