-- 1) Latest hour averages
SELECT device_id, year, month, day, hour,
       avg(temperature) AS temp_avg, avg(humidity) AS hum_avg, count(*) AS cnt
FROM iot_raw
GROUP BY device_id, year, month, day, hour
ORDER BY year DESC, month DESC, day DESC, hour DESC
LIMIT 100;

-- 2) Simple z-score anomaly (temperature) within device per day
WITH hourly AS (
  SELECT device_id, year, month, day, hour,
         avg(temperature) AS temp_avg
  FROM iot_raw
  GROUP BY device_id, year, month, day, hour
),
stats AS (
  SELECT device_id, year, month, day,
         avg(temp_avg) AS mu, stddev_samp(temp_avg) AS sigma
  FROM hourly
  GROUP BY device_id, year, month, day
)
SELECT h.*, (h.temp_avg - s.mu)/NULLIF(s.sigma,0) AS zscore
FROM hourly h
JOIN stats s USING (device_id, year, month, day)
WHERE ABS((h.temp_avg - s.mu)/NULLIF(s.sigma,0)) >= 3
ORDER BY ABS((h.temp_avg - s.mu)/NULLIF(s.sigma,0)) DESC
LIMIT 100;
