/* GOLD LAYER : Fatigue analysis and leaderboard
   Goal: Identify which athtlete is puhsing the hardest (acceleration) and determine there physiological zone (Heart rate zone)
   - note is that the ranking will be based on the timestamp and rankend between the 3 athletes

   Stack: 
    - CTE for readability
    - WINDOW FUNCTIONS(Lag, moving average, Rank)

*/


CREATE OR REPLACE VIEW `pulse_stream_lake.gold_fatigue_analysis` AS 
# Step 1: timestamp is a string need to convert to real TIMESTAMP object to perform calculations
WITH BaseData AS(
    SELECT
        athlete_id,
        name, 
        country, 
        timestamp,
        speed,
        heart_rate,
        location,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', timestamp) as event_time
    FROM `pulse-stream-core.pulse_stream_lake.silver_telemetry`
),

# Step 2 : Need previous rows data to calculate the change, LAG to look at previous row
CalculatedLag AS(
    SELECT *,
    --Get avergae speed from previous second
    LAG(speed) OVER(PARTITION BY athlete_id ORDER BY event_time) as prev_speed,
    -- Get exact time of pervioud record
    LAG(event_time) OVER (PARTITION BY  athlete_id ORDER BY event_time) as prev_event_time,
    FROM BaseData
),

# Step 3: Calculate the metrics (Physics and smoothing)
AthleteMetrics AS (
    SELECT *,
        TIMESTAMP_DIFF(event_time, prev_event_time, MILLISECOND)/1000 as time_diff_sec,

    --Smoothed Heart rate whcih is average of last ten seconds
        AVG(heart_rate) OVER (PARTITION BY athlete_id ORDER BY event_time
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as smooth_heart_rate
    FROM CalculatedLag
),

# Step 4 : Main business logic setting zones and status
FinalLogic AS (
    SELECT 
        athlete_id,
        name,
        country,
        event_time as timestamp,
        location,
        ROUND(speed, 2) as speed_kph,
        ROUND(smooth_heart_rate, 0) as hr_bpm,

        -- Physics : Accleration = (Delta Speed)/(Delta Time)
        ROUND(IEEE_DIVIDE(speed - prev_speed, time_diff_sec), 2) as accerleration_m_s2,

        -- Zones which classify the effort
        CASE 
            WHEN smooth_heart_rate >= 170 THEN '4. VO2 Max (Red)'
            WHEN smooth_heart_rate >= 150 THEN '3. Threshold (Orange)'
            WHEN smooth_heart_rate >= 120 THEN '2. Aerobic (Green)'
            ELSE '1. Recovery (Blue)'
        END as training_zone,

        -- Zones to see if the athlete is slowing down or speeding up
        CASE 
            WHEN (speed - prev_speed) > 0.5 THEN '🚀 ACCELERATING'
            WHEN (speed - prev_speed) < -0.5 THEN '🛑 BRAKING'
            ELSE "STEADY"
        END as movement_status

    FROM AthleteMetrics

)


-- Main output : Leaderborad 

SELECT *,
    -- Ranking Logic
    DENSE_RANK() OVER ( PARTITION BY TIMESTAMP_TRUNC(timestamp, SECOND)  ORDER BY speed_kph DESC
    ) as current_rank
FROM FinalLogic
ORDER BY timestamp DESC, current_rank ASC;










