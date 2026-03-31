CREATE DATABASE BIRD_SPECIES;
USE BIRD_SPECIES;
CREATE DATABASE IF NOT EXISTS bird_analysis;
USE bird_analysis;

DROP TABLE IF EXISTS bird_observations;

CREATE TABLE bird_observations (
    Admin_Unit_Code VARCHAR(50),
    Sub_Unit_Code VARCHAR(50),
    Plot_Name VARCHAR(100),
    Location_Type VARCHAR(50),
    Year INT,
    Date DATE,
    Start_Time TIME,
    End_Time TIME,
    Observer VARCHAR(100),
    Visit INT,
    Interval_Length VARCHAR(50),
    ID_Method VARCHAR(50),
    Distance VARCHAR(50),
    Flyover_Observed VARCHAR(10),            -- Changed from BOOLEAN
    Sex VARCHAR(50),
    Common_Name VARCHAR(100),
    Scientific_Name VARCHAR(100),
    AcceptedTSN DOUBLE,
    TaxonCode DOUBLE,
    AOU_Code VARCHAR(50),
    PIF_Watchlist_Status VARCHAR(10),        -- Changed from BOOLEAN
    Regional_Stewardship_Status VARCHAR(10), -- Changed from BOOLEAN
    Temperature DECIMAL(5,2),
    Humidity DECIMAL(5,2),
    Sky VARCHAR(50),
    Wind VARCHAR(150),
    Disturbance VARCHAR(100),
    Previously_Obs VARCHAR(20),
    Initial_Three_Min_Cnt VARCHAR(10),       -- Changed from BOOLEAN
    Source_Ecosystem VARCHAR(50),
    Source_Sheet VARCHAR(50),
    Site_Name VARCHAR(255),
    Month INT,
    Dist_Score INT
);
SET SESSION sql_mode = '';
set global local_infile =1;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/my_cleaned_dataset.csv' 
INTO TABLE bird_species.bird_observations
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

select * from bird_observations;
desc bird_observations;
-- 1. Create Dimension: Location
CREATE TABLE dim_location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    Admin_Unit_Code VARCHAR(50),
    Sub_Unit_Code VARCHAR(50),
    Plot_Name VARCHAR(100),
    Location_Type VARCHAR(50),
    Source_Ecosystem VARCHAR(50),
    Source_Sheet VARCHAR(50),
    Site_Name VARCHAR(255)
);

-- 2. Create Dimension: Species
CREATE TABLE dim_species (
    species_id INT AUTO_INCREMENT PRIMARY KEY,
    Common_Name VARCHAR(100),
    Scientific_Name VARCHAR(100),
    AcceptedTSN VARCHAR(50),
    TaxonCode VARCHAR(50),
    AOU_Code VARCHAR(50),
    PIF_Watchlist_Status VARCHAR(10),
    Regional_Stewardship_Status VARCHAR(10)
);

-- 3. Create Dimension: Weather & Environment
CREATE TABLE dim_environment (
    environment_id INT AUTO_INCREMENT PRIMARY KEY,
    Temperature DECIMAL(5,2),
    Humidity DECIMAL(5,2),
    Sky VARCHAR(50),
    Wind VARCHAR(150),
    Disturbance VARCHAR(100),
    Dist_Score INT
);

-- 4. Create Dimension: Date
CREATE TABLE dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    Observation_Date DATE,
    Observation_Year INT,
    Observation_Month INT
);

-- 5. Create Fact Table: Observations (Connecting everything with Foreign Keys)
CREATE TABLE fact_observations (
    observation_id INT AUTO_INCREMENT PRIMARY KEY,
    location_id INT,
    species_id INT,
    environment_id INT,
    date_id INT,
    Start_Time TIME,
    End_Time TIME,
    Observer VARCHAR(100),
    Visit INT,
    Interval_Length VARCHAR(50),
    ID_Method VARCHAR(50),
    Distance VARCHAR(50),
    Flyover_Observed VARCHAR(10),
    Sex VARCHAR(50),
    Previously_Obs VARCHAR(20),
    Initial_Three_Min_Cnt VARCHAR(10),
    
    -- Defining the Foreign Keys
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (species_id) REFERENCES dim_species(species_id),
    FOREIGN KEY (environment_id) REFERENCES dim_environment(environment_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- Insert Unique Locations
INSERT INTO dim_location (Admin_Unit_Code, Sub_Unit_Code, Plot_Name, Location_Type, Source_Ecosystem, Source_Sheet, Site_Name)
SELECT DISTINCT Admin_Unit_Code, Sub_Unit_Code, Plot_Name, Location_Type, Source_Ecosystem, Source_Sheet, Site_Name
FROM bird_observations;

-- Insert Unique Species
INSERT INTO dim_species (Common_Name, Scientific_Name, AcceptedTSN, TaxonCode, AOU_Code, PIF_Watchlist_Status, Regional_Stewardship_Status)
SELECT DISTINCT Common_Name, Scientific_Name, AcceptedTSN, TaxonCode, AOU_Code, PIF_Watchlist_Status, Regional_Stewardship_Status
FROM bird_observations;

-- Insert Unique Environments
INSERT INTO dim_environment (Temperature, Humidity, Sky, Wind, Disturbance, Dist_Score)
SELECT DISTINCT Temperature, Humidity, Sky, Wind, Disturbance, Dist_Score
FROM bird_observations;

-- Insert Unique Dates
INSERT INTO dim_date (Observation_Date, Observation_Year, Observation_Month)
SELECT DISTINCT Date, Year, Month
FROM bird_observations;

-- Populating the Fact Table (This joins the flat table to the dims to grab the new IDs)
INSERT INTO fact_observations (location_id, species_id, environment_id, date_id, Start_Time, End_Time, Observer, Visit, Interval_Length, ID_Method, Distance, Flyover_Observed, Sex, Previously_Obs, Initial_Three_Min_Cnt)
SELECT 
    l.location_id, s.species_id, e.environment_id, d.date_id,
    b.Start_Time, b.End_Time, b.Observer, b.Visit, b.Interval_Length, b.ID_Method, b.Distance, b.Flyover_Observed, b.Sex, b.Previously_Obs, b.Initial_Three_Min_Cnt
FROM bird_observations b
JOIN dim_location l ON b.Plot_Name = l.Plot_Name -- simplified join condition
JOIN dim_species s ON b.Common_Name = s.Common_Name
JOIN dim_environment e ON b.Temperature = e.Temperature AND b.Humidity = e.Humidity AND b.Sky = e.Sky
JOIN dim_date d ON b.Date = d.Observation_Date;

select * from dim_species ;
select * from fact_observations;

-- what are the top 3 most frequently observed bird species?-- 
with Rankedbirds as
(SELECT 
    l.Admin_Unit_Code, 
    s.Common_Name, 
    COUNT(f.observation_id) AS observation_count,
    dense_rank() over (partition by admin_unit_code order by  COUNT(f.observation_id) desc) as species_rank
FROM fact_observations f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_species s ON f.species_id = s.species_id
GROUP BY 
    l.Admin_Unit_Code, 
    s.Common_Name)
    select * from rankedbirds
    where species_rank <= 3;
    
   --  Create a running total of uniquely identified "At-Risk" birds observed over the course of the year.
   
   WITH FirstSightings AS (
    -- Step 1: Find the exact date each At-Risk bird was discovered
    SELECT 
        s.Common_Name,
        MIN(d.Observation_Date) AS discovery_date
    FROM fact_observations f
    JOIN dim_species s ON f.species_id = s.species_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE s.PIF_Watchlist_Status = 'True' 
    GROUP BY s.Common_Name
),

DailyDiscoveries AS (
    -- Step 2: Count how many *new* birds were found on each specific date
    SELECT 
        discovery_date,
        COUNT(Common_Name) AS new_birds_found
    FROM FirstSightings
    GROUP BY discovery_date
)

-- Step 3: Create the running total using a Window Function
SELECT 
    discovery_date,
    new_birds_found,
    SUM(new_birds_found) OVER (ORDER BY discovery_date) AS cumulative_at_risk_birds
FROM DailyDiscoveries
ORDER BY discovery_date;

-- For the "Field Sparrow", calculate the temperature difference between their current observation and 
-- their previous observation in the dataset.

SELECT 
    d.Observation_Date,
    f.Start_Time,
    e.Temperature,
    LAG(e.Temperature) OVER (ORDER BY d.Observation_Date, f.Start_Time) AS prev_temp,
    e.Temperature - LAG(e.Temperature) OVER (ORDER BY d.Observation_Date, f.Start_Time) AS temp_difference
FROM fact_observations f
JOIN dim_species s ON f.species_id = s.species_id
JOIN dim_environment e ON f.environment_id = e.environment_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE s.Common_Name = 'Field Sparrow'
ORDER BY d.Observation_Date, f.Start_Time;

-- What is the 90th percentile of humidity levels during which "Regional Stewardship" birds are observed?

WITH HumidityRanks AS (
    SELECT 
        e.Humidity,
        PERCENT_RANK() OVER (ORDER BY e.Humidity) as pct_rank
    FROM fact_observations f
    JOIN dim_species s ON f.species_id = s.species_id
    JOIN dim_environment e ON f.environment_id = e.environment_id
    WHERE s.Regional_Stewardship_Status = 'True'
)
SELECT MIN(Humidity) AS 90th_percentile_humidity
FROM HumidityRanks
WHERE pct_rank >= 0.90;

-- Which plots (Plot_Name) have a higher total bird count than 
-- the overall average bird count across all plots?

WITH PlotTotals AS (
    SELECT location_id, COUNT(observation_id) as total_birds
    FROM fact_observations
    GROUP BY location_id
),
OverallAverage AS (
    SELECT AVG(total_birds) as avg_birds FROM PlotTotals
)
SELECT l.Plot_Name, pt.total_birds
FROM PlotTotals pt
JOIN dim_location l ON pt.location_id = l.location_id
CROSS JOIN OverallAverage oa
WHERE pt.total_birds > oa.avg_birds;

-- Identify species that were observed less than 5 times in the entire dataset, and list the specific 
-- Ecosystems where they managed to be found.

WITH SpeciesCounts AS (
    SELECT species_id, COUNT(*) as sighting_count
    FROM fact_observations
    GROUP BY species_id
    HAVING COUNT(*) < 5
)
SELECT DISTINCT s.Common_Name, l.Source_Ecosystem, sc.sighting_count
FROM SpeciesCounts sc
JOIN fact_observations f ON sc.species_id = f.species_id
JOIN dim_species s ON sc.species_id = s.species_id
JOIN dim_location l ON f.location_id = l.location_id;

-- Which observers logged more than 50 birds for at least 3 consecutive days?

WITH DailyTotals AS (
    SELECT f.Observer, d.Observation_Date, COUNT(f.observation_id) as daily_birds
    FROM fact_observations f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY f.Observer, d.Observation_Date
    HAVING COUNT(f.observation_id) > 50
),
StreakCheck AS (
    SELECT 
        Observer,
        Observation_Date,
        LEAD(Observation_Date, 1) OVER(PARTITION BY Observer ORDER BY Observation_Date) as next_day,
        LEAD(Observation_Date, 2) OVER(PARTITION BY Observer ORDER BY Observation_Date) as third_day
    FROM DailyTotals
)
SELECT DISTINCT Observer
FROM StreakCheck
WHERE DATEDIFF(next_day, Observation_Date) = 1 
  AND DATEDIFF(third_day, Observation_Date) = 2;
  
-- Find the species that were observed in Grasslands but were never observed in Forests.

SELECT DISTINCT s.Common_Name
FROM fact_observations f
JOIN dim_species s ON f.species_id = s.species_id
JOIN dim_location l ON f.location_id = l.location_id
WHERE l.Source_Ecosystem = 'Grassland'
AND s.species_id NOT IN (
    SELECT DISTINCT f2.species_id
    FROM fact_observations f2
    JOIN dim_location l2 ON f2.location_id = l2.location_id
    WHERE l2.Source_Ecosystem = 'Forest'
);

-- For each park, calculate the ratio of "At-Risk" birds to standard birds as a percentage.

SELECT 
    l.Admin_Unit_Code,
    COUNT(f.observation_id) as total_birds,
    SUM(CASE WHEN s.PIF_Watchlist_Status = 'True' THEN 1 ELSE 0 END) as at_risk_birds,
    (SUM(CASE WHEN s.PIF_Watchlist_Status = 'True' THEN 1 ELSE 0 END) / COUNT(f.observation_id)) * 100 AS priority_ratio_pct
FROM fact_observations f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_species s ON f.species_id = s.species_id
GROUP BY l.Admin_Unit_Code;

-- What specific hour of the day (e.g., 6 AM, 7 AM) has the highest volume of birds identified 
-- via the "Singing" method?

SELECT 
    EXTRACT(HOUR FROM Start_Time) AS hour_of_day,
    COUNT(observation_id) AS total_singing_birds
FROM fact_observations
WHERE ID_Method = 'Singing'
GROUP BY EXTRACT(HOUR FROM Start_Time)
ORDER BY total_singing_birds DESC
LIMIT 1;
-- What percentage of observations in Grasslands are "Flyovers" compared
--  to "Active" (non-flyover) observations?

SELECT 
    l.Source_Ecosystem,
    COUNT(f.observation_id) as total_observations,
    SUM(CASE WHEN f.Flyover_Observed = 'True' THEN 1 ELSE 0 END) as flyovers,
    (SUM(CASE WHEN f.Flyover_Observed = 'True' THEN 1 ELSE 0 END) / COUNT(f.observation_id)) * 100 AS flyover_pct
FROM fact_observations f
JOIN dim_location l ON f.location_id = l.location_id
WHERE l.Source_Ecosystem = 'Grassland'
GROUP BY l.Source_Ecosystem;


--  Find instances where the exact same species was observed in the exact 
--  same Plot_Name, but on a completely different Visit number.

SELECT DISTINCT
    s.Common_Name,
    l.Plot_Name,
    f1.Visit as visit_one,
    f2.Visit as visit_two
FROM fact_observations f1
JOIN fact_observations f2 
    ON f1.species_id = f2.species_id 
    AND f1.location_id = f2.location_id
JOIN dim_species s ON f1.species_id = s.species_id
JOIN dim_location l ON f1.location_id = l.location_id
WHERE f1.Visit != f2.Visit;

--  List pairs of different Observers who logged observations in the exact 
--  same Plot_Name on the exact same Date.

 SELECT DISTINCT
    f1.Observer AS observer_a,
    l.Plot_Name,
    d.Observation_Date AS first_visit,
    d2.Observation_Date AS next_visit
FROM fact_observations f1

JOIN fact_observations f2 
    ON f1.location_id = f2.location_id 
JOIN dim_location l ON f1.location_id = l.location_id
JOIN dim_date d ON f1.date_id = d.date_id

JOIN dim_date d2 ON f2.date_id = d2.date_id
WHERE f1.Observer = f2.Observer 
AND f1.date_id < f2.date_id;

 -- Calculate a 7-day moving average of total daily bird observations across the whole dataset.
 
 WITH DailyTotals AS (
    SELECT 
        d.Observation_Date, 
        COUNT(f.observation_id) AS daily_count
    FROM fact_observations f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.Observation_Date
)
SELECT 
    Observation_Date,
    daily_count,
    AVG(daily_count) OVER (
        ORDER BY Observation_Date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7_day
FROM DailyTotals;
 
 -- What is the average duration (in minutes) between Start_Time and End_Time for observations where the Disturbance
 -- was marked as having a "Serious effect"?
 
 SELECT 
    AVG(TIMESTAMPDIFF(MINUTE, f.Start_Time, f.End_Time)) AS avg_duration_minutes
FROM fact_observations f
JOIN dim_environment e ON f.environment_id = e.environment_id
WHERE e.Disturbance = 'Serious effect on count';