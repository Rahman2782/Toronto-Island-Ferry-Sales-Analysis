# Toronto Island Ferry Ticket Sales - End-to-End SQL Data Analysis Project
This project showcases data cleaning, transformation, and exploratory analysis to extract business insights.
### Skills Demonstrated:
- SQL Data Cleaning (duplicates, missing data, timestamp normalization)
- Window Functions (RANK, LAG, ROW_NUMBER)
- CTEs + Recursive CTEs
- Aggregations and Date/Time functions
- Data modeling (daily + monthly summary tables)
- Exploratory Analysis + Business Insights
- Seasonality and demand forecasting
- Building analytics-ready datasets

### Tools Used:
- MySQL,
- Excel,
- SQL Workbench,
- GitHub
- Tableau (coming soon)
  
## Project Overview
This project analyzes Toronto Island Ferry ticket sales and redemptions over 10 years of data from 2015–2025.

The goal of this analysis is to:
- Clean and prepare raw CSV data for accurate analysis
- Extract patterns in ticket demand
- Identify seasonal trends, daily/hourly peak times, and operational insights
- Build aggregated datasets for dashboards and reporting

## Dataset Summary 
- The data shows ticket sales and redemptions in 15 minute time intervals everyday (ex. the value of sales_count would be all sales made from 8:00am - 8:15 am on a certain date)
- 259,501 unique rows
  
Columns before cleaning:
| COLUMN | DESCRIPTION |
| ------ | ----------- |
| id | unique id |
| timestamp | date/time string "DD/MM/YYYY HH:MM" |
| redemption | tickets redeemed |
| sales_count | tickets sold | 

## Data Cleaning
- To check for any duplicate rows, a window function was used to count for any rows with identical values
```
  WITH cte AS (
  	SELECT *, ROW_NUMBER() OVER (PARTITION BY timestamp, redemption, sales_count) AS rn
      FROM t_island_ferry_sales
  )
  select * from cte 
  WHERE rn > 1; -- no duplicate rows found
```
- Checking for any missing days using a CTE. By using a WITH RECURSIVE block, a complete calendar list was built to get all possible days from the time range of the dataset. It begins with the starting point, which is the earliest date in the series (dt) and now the first day in the new calendar. Next, the loop takes the current day (dt) value and adds 1 day to it, this loop stops when 'dt' is no longer less than the latest date in the dataset ( MAX(date) ).
- After the recursive block, I took every row from the calendar (c) and try to match it with a date in the sales dataset (t), this is achieved by using a LEFT JOIN. If there is a matchm the join passes and the t.date column will have a value, if not then the field will be left as NULL.
- Only rows without matches with the calendar and original dataset are returned.
```
WITH RECURSIVE calendar AS (
    SELECT MIN(date) AS dt
    FROM t_island_ferry_sales
    
    UNION ALL
    
    SELECT DATE_ADD(dt, INTERVAL 1 DAY)
    FROM calendar
    WHERE dt < (SELECT MAX(date) FROM t_island_ferry_sales)
)
SELECT c.dt AS missing_date
FROM calendar c
LEFT JOIN t_island_ferry_sales t
    ON t.date = c.dt
WHERE t.date IS NULL;

SET @@cte_max_recursion_depth = 30000;
```
|missing_dates| 
|-------------|
|2015-05-03|
|2018-01-04|
|2015-05-02|
|2025-01-26|
|2021-02-21|
|2021-02-22|
- A recursive calendar check revealed 6 missing dates, meaning certain days have no recorded transactions, not necessarily “zero sales.”

- During data cleaning, timestamp was seperated into 2 columns, date (dd/mm/YYYY) and time (HH:MM). This was done to make future analysis easier in terms of aggregate functions or in the creation of new tables.
```
ALTER TABLE t_island_ferry_sales
ADD COLUMN date DATE;
ADD COLUMN time TIME;

UPDATE t_island_ferry_sales
SET 
  `date` = DATE(timestamp),
  `time` = TIME(timestamp);

ALTER TABLE t_island_ferry_sales
DROP COLUMN timestamp;
```
- A season classification column was also added for business insights. A switch case was used here, the corresponding season is assigned to the date based on the month.
```
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
```

- To create dashboards, two new tables were created, daily_sales and monthly_sales.
```
CREATE TABLE daily_sales AS 
SELECT DATE(date) AS sale_day, SUM(sales_count) AS total_daily_sales
FROM t_island_ferry_sales
GROUP BY DATE(date);

CREATE TABLE monthly_sales AS
SELECT DATE_FORMAT(sale_day, '%M') AS month,
       YEAR(sale_day) AS year,
       SUM(total_daily_sales) AS total_monthly_sales
FROM daily_sales
GROUP BY month, year;
```


## 2. Exploratory Data Analysis
### 2.1 Total Sales & Redemptions per Year
```
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
```
<img width="179" height="208" alt="image" src="https://github.com/user-attachments/assets/9de1ecf9-cea1-4cf0-8c52-56030c04bcbc" />


Insights:
- Best year: 2016 w/ 1,518,428 ticket sales and 1,425,779 redemptions
- Worst year: 2020 w/ 366,606 ticket sales and 374,546 redemptions

### 2.2 Monthly Sales Trends - Average Sales per Month and Total Monthly Sales
```
SELECT 
	month_name,
    ROUND(AVG(monthly_total)) AS avg_montly_sales,
    SUM(monthly_total) AS total_sales_across_years
FROM (
	SELECT 
		YEAR(date) as year,
		MONTH(date) as month_num,
		DATE_FORMAT(date, '%M') as month_name,
		SUM(sales_count) AS monthly_total
	FROM t_island_ferry_sales
	GROUP BY year, month_num, month_name
) AS montlhy_data 
GROUP BY month_name
ORDER BY total_sales_across_years DESC;
```
Insights (ordered most to least sales per category):
Top-selling months: Summer
- **August:** Total Sales = 3,442,762 | Average Sales = 312,978 
- **July:** Total Sales = 3,083,858 | Average Sales = 280,351
- **June:** Total Sales = 1,814,395 | Average Sales = 164,945
- Total Sales this Season: 8,341,015
*Summer drives 64.39% of annual revenue. This indicates ferry staffing, scheduling, and vessel allocation must prioritize June–August.*

Lowest-selling months: Winter 
- **December:** Total Sales = 152,728 | Average Sales = 15,273
- **February:** Total Sales = 146,179 | Average Sales = 14,618
- **January:** Total Sales = 130,455 | Average Sales = 13,046
- Total Sales this Season: 429,362 - Roughly 3.31% of total sales are from winter
*Winter only brought in 3.31% of annual revenue.*

Lifetime Sales: 12,954,164

### 2.3 Peak Hour Demand (All-Time)
```
SELECT
    hour,
    total_sales,
    LAG(total_sales) OVER (ORDER BY total_sales DESC) AS previous_sales,
    ROUND(
        (total_sales - LAG(total_sales) OVER (ORDER BY total_sales DESC))
        / LAG(total_sales) OVER (ORDER BY total_sales DESC) * 100,
        2
    ) AS percent_drop
FROM hourly
ORDER BY total_sales DESC;
```
<img width="309" height="430" alt="image" src="https://github.com/user-attachments/assets/854efa17-05c4-47e8-919b-8259024cc943" />

- This returns the hours of the day with the most and least amount of sales over the last 10 years. The percent difference between the hours as they descend also reveal key insights.

- 12 PM is peak hour with the highest lifetime ticket sales
- 11 AM, 12 PM, 1 PM, 2 PM show less than 3% variance, forming a stable peak demand block
- The first major dip occurs at 3 PM (−10.28%)
- Overnight hours (12 AM–6 AM) show a 46.48% decline from the evening peak
- 4 AM is the weakest hour with only 3,426 lifetime sales

**Key Insights:**
- The 11 AM–2 PM window contributes 37% of daily sales, with a steep drop-off of 22–30% after 3 PM.
- Promotion timing, staffing, and ferry frequency should focus on late morning to early afternoon.

## Conclusion 
**Key Takeaways for Toronto Island Ferry Operations:**
- Summer accounts for 64% of lifetime ticket revenue, confirming a highly seasonal demand pattern.
- Peak demand occurs from 11 AM–2 PM, representing 37% of all daily sales.
- Winter accounts for only 3% of revenue, suggesting predictable low utilization periods.
- A sudden 10.28% drop occurs at 3 PM, marking the end of the operational peak window.
- Only six dates were missing from the dataset, confirming accurate operational coverage except during closure periods.

# Dashboards coming soon
