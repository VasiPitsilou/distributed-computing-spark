# Computation of MBA players statistics using Apache Spark

<img width="736" height="736" alt="image" src="https://github.com/user-attachments/assets/84b85f9e-c6e9-4a11-b24a-071840f5d16e" />

Program that outputs statistics of MBA players that result from analyzing gigabytes of data including the players' position coordinates, four times a second. 
Apache Spark is used, building was performed with Maven.

## Task 1: Calculate the distance travelled
For each player, find the distance travelled during their whole stay on the field and normalize it to find average distance the player covers per quarter. Output results in `distance_per_player.csv`.

## Task 2: Compute distance zones
Compute the players' instantaneous speed and classify their runs into slow, normal and fast. For each player compute the number of runs, the total distance travelled per speed zone, output results in `speed_zones.csv` file.

## Task 3: Rebounds
Count the number of offensive and defensive rebounds for each player and determine the location of the farthest rebound from the basket for each player. Output results in `rebounds.csv` file.


_The project was completed for the Big Data Analytics Programming course, Master of Artificial Intelligence, KU Leuven (2024-2025)._
