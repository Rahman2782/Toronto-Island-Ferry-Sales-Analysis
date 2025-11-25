# Toronto Island Ferry Ticket Sales - End-to-End SQL Data Analysis Project
This project was designed to showcase data cleaning, transformation, and exploratory analysis to extract buisness insights
Tools Used: MySQL • SQL Window Functions • Data Cleaning • Aggregations • Date/Time Transformations

## Project Overview
This project analyzes Toronto Island Ferry ticket sales and redemptions over from 2015 to November 2025, 10 years worth of data.

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
- During data cleaning, timestamp was seperated into 2 columns, date (dd/mm/YYYY) and time (HH:MM)
```
ALTER TABLE t_island_ferry_sales
ADD date DATE;
ADD time TIME;

UPDATE t_island_ferry_sales
SET 
  `date` = DATE(timestamp),
  `time` = TIME(timestamp);
```

- 

```
  WITH cte AS (
  	SELECT *, ROW_NUMBER() OVER (PARTITION BY timestamp, redemption, sales_count) AS rn
      FROM t_island_ferry_sales
  )
  select * from cte 
  WHERE rn > 1; -- no duplicate rows found
```
