-- GROUP B (FORMULA 1) DATABASE CREATION

CREATE DATABASE F1_db;

USE F1_db;

CREATE TABLE IF NOT EXISTS CIRCUITS (
          circuitId INT PRIMARY KEY,
          circuitRef VARCHAR(40),
          name VARCHAR(80),
          location VARCHAR(100),
          country VARCHAR(80));

CREATE TABLE IF NOT EXISTS DRIVERS (
          driverId INT PRIMARY KEY,
          driverRef VARCHAR(40) UNIQUE,
          number INT,
          name VARCHAR(80),
          surname VARCHAR(80),
          dateOfBirth DATE,
          nationality VARCHAR(80));

CREATE TABLE IF NOT EXISTS RACES (
          raceId INT PRIMARY KEY,
          raceYear INT,
          raceNumber INT,
          circuitId INT,
          raceName VARCHAR(80),
          raceDate DATE,
          raceTime TIME,
          CONSTRAINT RACES_ibfk_4 FOREIGN KEY (circuitId)
                REFERENCES CIRCUITS (circuitId) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS RESULTS (
          resultId INT PRIMARY KEY,
          raceId INT,
          driverId INT,
          gridPos INT,
          finalPos INT,
          points FLOAT,
          fastLap INT,
          CONSTRAINT RESULTS_ibfk_2 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT RESULTS_ibfk_3 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS LAPTIMES (
          raceId INT,
          driverId INT,
          lap INT,
          position INT,
          lapTime VARCHAR(15),
          ms INT,
          PRIMARY KEY (raceId, driverId, lap),
          CONSTRAINT LAPTIMES_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT LAPTIMES_ibfk_2 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS PITSTOPS (
          raceId INT,
          driverId INT,
          stopNumber INT,
          lapNumber INT,
          timePitStop TIME,
          duration FLOAT,
          PRIMARY KEY (raceId, driverId, stopNumber),
          CONSTRAINT PITSTOPS_ibfk_1 FOREIGN KEY (raceId)
            REFERENCES RACES (raceId) ON DELETE CASCADE,
          CONSTRAINT PITSTOPS_ibfk_2 FOREIGN KEY (driverId)
            REFERENCES DRIVERS (driverId) ON DELETE CASCADE);