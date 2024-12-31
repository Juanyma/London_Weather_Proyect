select *
from london_weather_raw
ORDER BY Q_TX;

CREATE TABLE london_weather_staging
LIKE london_weather_raw;

INSERT INTO london_weather_staging
SELECT *
FROM london_weather_raw;

-- created a staging data to work with

SELECT *
FROM london_weather_staging;

ALTER TABLE london_weather_staging
RENAME COLUMN `CC`
TO `daily_cloud_cover_oktas`;

SELECT DATE(from_unixtime(`DATE`)) as ss
FROM london_weather_staging;

ALTER TABLE london_weather_staging
ADD COLUMN date_taken DATE ;

UPDATE london_weather_staging
SET date_taken = str_to_date(CAST(`DATE` AS CHAR(8)), '%Y%m%d');

ALTER TABLE london_weather_staging
MODIFY COLUMN date_taken date first;

ALTER TABLE london_weather_staging
DROP COLUMN `DATE`;

SELECT * 
FROM london_weather_staging;

-- renamed all important columns and standarizing date

CREATE TABLE london_weather_staging2
LIKE london_weather_staging;

INSERT INTO london_weather_staging2
SELECT *
FROM london_weather_staging;

-- created new staging

SELECT *
FROM london_weather_staging2
LIMIT 20000;

ALTER TABLE london_weather_staging2
MODIFY `daily_global_radiation_w/m2` INT;

SELECT date_taken,`daily_precipitation_.mm`, round((`daily_precipitation_.mm` * .1),1)
FROM london_weather_staging2 LIMIT 20000;

ALTER TABLE london_weather_staging2
ADD COLUMN `daily_precipitation_.mm2` DOUBLE AFTER `daily_precipitation_.mm`;

UPDATE london_weather_staging2
SET `daily_precipitation_.mm2` = round((`daily_precipitation_.mm` * .1),1);

ALTER TABLE london_weather_staging3
DROP COLUMN `daily_precipitation_.mm`;

ALTER TABLE london_weather_staging2
RENAME COLUMN `daily_precipitation_.mm2`
TO `daily_precipitation_mm`;


CREATE TABLE london_weather_staging3
LIKE london_weather_staging2;

INSERT INTO london_weather_staging3
SELECT *
FROM london_weather_staging2;


SELECT 
    daily_sunshine_duration_hs,
    CONCAT(
    FLOOR(daily_sunshine_duration_hs), ':',
    LPAD(ROUND((daily_sunshine_duration_hs - FLOOR(daily_sunshine_duration_hs)) * 60), 2, '0')
    ) AS formatted_time
FROM london_weather_staging3;

ALTER TABLE london_weather_staging3
ADD COLUMN `daily_sunshine_duration_hs_formated` TIME AFTER `daily_sunshine_duration_hs`;
    
UPDATE london_weather_staging3
SET daily_sunshine_duration_hs_formated = CONCAT(
    FLOOR(daily_sunshine_duration_hs), ':',
    LPAD(ROUND((daily_sunshine_duration_hs - FLOOR(daily_sunshine_duration_hs)) * 60), 2, '0')
)
WHERE daily_sunshine_duration_hs IS NOT NULL;

UPDATE london_weather_staging3
SET daily_sunshine_duration_hs_formated = SUBSTRING(daily_sunshine_duration_hs_formated, 1, 5)
WHERE daily_sunshine_duration_hs_formated IS NOT NULL;

-- standardised all data

CREATE TABLE london_weather_staging4
LIKE london_weather_staging3;

INSERT INTO london_weather_staging4
SELECT *
FROM london_weather_staging3;


SELECT *
FROM london_weather
LIMIT 20000;

/* WITH duplicate_cte AS
(SELECT *, 
ROW_NUMBER() OVER(PARTITION BY date_taken) as row_num
FROM london_weather_staging2
LIMIT 20000)
select * from duplicate_cte WHERE row_num =1; */
-- MADE SURE THAT THERE ARE NO DUPLICATES


SELECT *
FROM london_weather_staging4
WHERE (Q_TX =1 OR Q_TN = 1 OR Q_TG = 1 OR Q_SS = 1 OR Q_SD = 1 OR Q_RR = 1 OR Q_QQ = 1 OR Q_PP = 1 OR Q_HU = 1 OR Q_CC = 1)
LIMIT 20000;

SELECT *,
	CASE
		WHEN (Q_TX =1 OR Q_TN = 1 OR Q_TG = 1 OR Q_SS = 1 OR Q_SD = 1 OR Q_RR = 1 OR Q_QQ = 1 OR Q_PP = 1 OR Q_HU = 1 OR Q_CC = 1)
		THEN "suspect_data"
		ELSE "valid_data"
	END as flag_Q
FROM london_weather_staging2
limit 20000;

UPDATE london_weather_staging4
SET flag = CASE
		WHEN (Q_TX =1 OR Q_TN = 1 OR Q_TG = 1 OR Q_SS = 1 OR Q_SD = 1 OR Q_RR = 1 OR Q_QQ = 1 OR Q_PP = 1 OR Q_HU = 1 OR Q_CC = 1)
		THEN "suspect_data"
		ELSE "valid_data"
end;

ALTER TABLE london_weather_staging4
DROP COLUMN Q_TX,
DROP COLUMN Q_TN,
DROP COLUMN Q_TG,
DROP COLUMN Q_SS,
DROP COLUMN Q_SD,
DROP COLUMN Q_RR,
DROP COLUMN Q_QQ,
DROP COLUMN Q_PP,
DROP COLUMN Q_HU,
DROP COLUMN Q_CC;

ALTER TABLE london_weather_staging2
RENAME COLUMN `daily_precipitation_.mm2`
TO `daily_precipitation_mm`;

ALTER TABLE london_weather_staging4
RENAME TO  london_weather;

-- final cleaning & renaming