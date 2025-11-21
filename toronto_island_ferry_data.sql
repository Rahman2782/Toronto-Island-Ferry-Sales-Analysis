USE toronto;

CREATE TABLE t_island_ferry_sales (
	id int primary key,
    timestamp text,
    redemption int,
    sales_count int
);

SET GLOBAL LOCAL_INFILE = ON;
LOAD DATA LOCAL INFILE "C:/Users/Rahma/Desktop/Datasets/Toronto Island Ferry Ticket Counts.csv" into table t_island_ferry_sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

SELECT COUNT(*) from t_island_ferry_sales;
DESCRIBE t_island_ferry_sales;

SELECT * FROM t_island_ferry_sales WHERE sales_count IS NULL or sales_count = ''; -- having 0s is normal here

WITH cte AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY timestamp, redemption, sales_count) AS rn
    FROM t_island_ferry_sales
)
select * from cte 
WHERE rn > 1; -- no duplicate rows found

SELECT * FROM t_island_ferry_sales
LIMIT 20;

SELECT `timestamp`, STR_TO_DATE(`timestamp`, '%d/%m/%Y %H:%i')
from t_island_ferry_sales;

SET SQL_SAFE_UPDATES = 0;
UPDATE t_island_ferry_sales
SET `timestamp` = str_to_date(`timestamp`, '%d/%m/%Y %H:%i');
SET SQL_SAFE_UPDATES = 1;

SELECT * FROM t_island_ferry_sales
ORDER BY id desc;

ALTER TABLE t_island_ferry_sales
ADD `date` DATE,
ADD `time` TIME;

UPDATE t_island_ferry_sales
SET 
	`date` = DATE(timestamp),
    `time` = TIME(timestamp);
    
SELECT * FROM t_island_ferry_sales;

ALTER TABLE t_island_ferry_sales
DROP COLUMN timestamp;

-- adding a column to identify the season of sales, will make analysis easier
ALTER TABLE t_island_ferry_sales 
ADD COLUMN season TEXT;

UPDATE t_island_ferry_sales 
SET season =
	CASE 
		WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'N/A'
	END;
    
SELECT DISTINCT season
FROM t_island_ferry_sales;
-- ============================================================================ -- 

SELECT * FROM t_island_ferry_sales
WHERE YEAR(date) = 2025 AND MONTH(date) = 09
ORDER BY id desc;

SELECT * FROM t_island_ferry_sales;

-- calculates total sales and redemptions per year
SELECT 
	year(date), 
    SUM(sales_count) as total_sales, 
    SUM(redemption) as total_redemptions
FROM t_island_ferry_sales
GROUP BY YEAR(date);

-- view the months with the highest and lowest sales by year
SELECT 
	DATE_FORMAT(date, '%M') as month_name,
    SUM(sales_count) AS total_sales
FROM t_island_ferry_sales
WHERE YEAR(date) = 2025
GROUP BY month_name
ORDER BY total_sales desc;

-- to get the monthly average of tickets sold over the last 10 years, a new table needs to be created
CREATE TABLE daily_sales AS 
	SELECT 
		DATE(date) AS sale_day,
        SUM(sales_count) as total_daily_sales
	FROM t_island_ferry_sales
	GROUP BY DATE(date);

-- verifying data 
SELECT `date`, sales_count
FROM t_island_ferry_sales
WHERE date = '2025-11-19';

SELECT * FROM daily_sales;

SELECT *
FROM daily_sales
WHERE year(sale_day) = 2025
ORDER BY 2 desc;

-- years with most sales (ranked)
-- the inner query gets the total ticket sales per year
-- the outer query ranks the years by sales
SELECT 
	year,
    total_sales,
    RANK() OVER (ORDER BY total_sales DESC) as sales_rank
FROM (
	SELECT 
		year(sale_day) as year,
		SUM(total_daily_sales) as total_sales
	FROM daily_sales
	GROUP BY YEAR(sale_day)
) AS yearly_totals;

-- creating a new table for total monthly sales over 10 years
SELECT 
	DATE_FORMAT(sale_day, '%M') as month,
	YEAR(sale_day) as year,
    SUM(total_daily_sales) as total_monthly_sales
FROM daily_sales
GROUP BY month, year;

CREATE TABLE monthly_sales AS
	SELECT 
		DATE_FORMAT(sale_day, '%M') as month,
		YEAR(sale_day) as year,
		SUM(total_daily_sales) as total_monthly_sales
	FROM daily_sales
	GROUP BY month, year;

SELECT * FROM monthly_sales;

-- ranking the months that sold the most tickets over 10 years
SELECT 
	`month`,
    tms,
    RANK() OVER (ORDER BY tms DESC) as sales_rank
FROM (
	SELECT
		`month` as month,
        SUM(total_monthly_sales) as tms
	FROM monthly_sales
    GROUP BY month
) as montlhy_totals;

-- the hours of the day that sell the most tickets (and the least)
SELECT 
    HOUR(time) AS hour_of_day,
    SUM(sales_count) AS total_sales
FROM t_island_ferry_sales
GROUP BY HOUR(time)
ORDER BY total_sales desc;

-- lifetime sales
SELECT SUM(sales_count) FROM t_island_ferry_sales;

-- next, focus on redemption instead of sales because that is the true measure of immediate operational demand