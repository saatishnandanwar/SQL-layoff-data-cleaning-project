create database layoff;
use layoff;
CREATE TABLE layoffs (
    company VARCHAR(255),
    location VARCHAR(255),
    industry VARCHAR(255),
    total_laid_off INT,
    percentage_laid_off VARCHAR(50),
    date DATE,
    stage VARCHAR(100),
    country VARCHAR(100),
    funds_raised_millions DECIMAL(12,2)
);
select * from layoffs;

-- first thing we want to do is create a staging table.
-- This is the one we will work in and clean the data.
-- we want a table with the raw data in case something happens

-- We have to create a duplicate table to avoid any changes in the main table --

create table layoffs_staging
LIKE layoffs;

insert layoffs_staging
select*from layoffs; -- run this together

select * from layoffs_staging;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what
-- 4. remove any columss and rows that are not necessary - few ways

-- 1. find duplicates using partition

select company, industry, total_laid_off, 'date',
row_number() over 
(
partition by company, industry, total_laid_off, 'date')
as row_num
from layoffs_staging;

select * from (
select company, industry, total_laid_off, 'date',
row_number() over 
(
partition by company, industry, total_laid_off, 'date')
as row_num
from layoffs_staging
)
duplicates
where
row_num > 1;

-- these are our real duplicates

select * from (
select company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions,
row_number() over 
(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
duplicates
where
row_num > 1;
select * from layoffs_staging; -- after removing the duplicates --

-- these are the ones we want to delete where the row number is > 1 or 2 or greter essentially --below code gives a target table error
with delete_cte as
(
select * from (
select company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions,
row_number() over 
(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions)
as row_num
from layoffs_staging
)
duplicates
where
row_num > 1
)
delete
from delete_cte

alter table layoffs_staging add row_num int;




CREATE TABLE layoffs_staging2 (
company text,
`location`text,
`industry`text,
total_laid_off INT,
percentage_laid_off text,
date text,
`stage`text,
country text,
funds_raised_millions int,
row_num INT
);
 
INSERT INTO layoffs_staging2 (
company,
location,
industry,
total_laid_off,
percentage_laid_off,
date,
stage,
country,
funds_raised_millions,
row_num)
SELECT company,
location,
industry,
total_laid_off,
percentage_laid_off,
date,
stage,
country,
funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,
            percentage_laid_off,date, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;
        
select * from layoffs_staging2;	

-- now that we have this we can delete rows where row_num is greater than 2

delete from layoffs_staging2	
where row_num >=2;
 set sql_safe_updates = 0;

select * from layoffs_staging2;	

-- 2 Standardize data
select *
from layoffs_staging2;

-- if we look at industry it looks like we have some null
-- and empty rows, lets take a look at these
select distinct industry
from layoffs_staging2
order by industry;

select *
from layoffs_staging2
where industry is null
or industry = ''
order by industry;

select *
from layoffs_staging2
where company like 'Bally%';
-- nothing wrong here
select *
from layoffs_staging2
where company like 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I ma sure its the same for the others. what we can do is
-- write a query that if there is another row with the same company name,
-- It will update it to the non-null industry values
-- makes it easy so if there were thousands we wouln't have to manually check them all
-- we should set the blanks to nulls since those
-- are typically easier to work with
update layoffs_staging2
set industry = null
where industry = '';

-- now if we check those are all null
select *
from layoffs_staging2
where industry is null
or industry = ''
order by industry;

-- now we need to populate those nulls if possible
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where industry is null
or industry = ''
order by industry;

-- I also noticed the crypto has multiple different variations.
-- We need to standardize that - let's say all to crypto

select distinct industry
from layoffs_staging2
order by industry;

update layoffs_staging2
set industry = 'Crypto'
Where industry in ('Crypto Currency', 'CryptoCurrency');

-- now thats taken care of:
select distinct industry
from layoffs_staging2
order by industry;

-- we also need to look at

select *
from layoffs_staging2;

-- everything looks good except apparently
-- we have some "united states" and "united states."
-- with a period at end . lets standardize this.

select distinct country
from layoffs_staging2
order by country;

update layoffs_staging2
set country = trim(trailing '.' from country); ---- #this query will successfully remove any trailing periods from the country column.

-- Let's also fix the date columns:
SELECT *
FROM layoffs_staging2;
 
-- we can use str to date to update this field
-- UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y'); -- gives unknown system variable date-- 
 
-- now we can convert the date type properly
UPDATE layoffs_staging2
SET date =
    CASE
        -- Handle M/D/YYYY or MM/DD/YYYY
        WHEN date REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
            THEN STR_TO_DATE(date, '%m/%d/%Y')
 
        -- Handle YYYY-MM-DD 
        WHEN date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN date
 
        -- NULL or invalid stays NULL
        ELSE NULL
    END;
 
 
SET SQL_SAFE_UPDATES = 0;
 
SELECT *
FROM layoffs_staging2;

-- 3. Look at Null Values
 
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions 

--- all look normal. I don't think I want to change that

-- I like having them null because it makes it easier for calculations during the EDA phase
 
-- so there isn't anything I want to change with the null values

SELECT *
FROM layoffs_staging2
where total_laid_off is Null;

-- Delete useless data we cant't reallu use
delete FROM layoffs_staging2
where total_laid_off is Null
And percentage_laid_off is null;


SELECT *
FROM layoffs_staging2;



-- EDA
 
-- Here we are jsut going to explore the data and find trends or patterns or 

   -- anything interesting like outliers
 
-- normally when you start the EDA process you have some idea of what you're looking for
 
-- with this info we are just going to look around and see what we find!

SELECT MAX(total_laid_off)

FROM layoffs_staging2;
 
 
 
-- Looking at Percentage to see how big these layoffs were

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)

FROM layoffs_staging2

WHERE  percentage_laid_off IS NOT NULL;
 
-- Which companies had 1 which is basically 100 percent of they company laid off

SELECT *

FROM layoffs_staging2

WHERE  percentage_laid_off = 1;

-- these are mostly startups it looks like who all went 

-- out of business during this time
 
-- if we order by funds_raised_millions we can see

--  how big some of these companies were

SELECT *

FROM layoffs_staging2

WHERE  percentage_laid_off = 1

ORDER BY funds_raised_millions DESC;

select company, total_laid_off
from layoffs_staging
order by 2 DESC
limit 5;
-- now that just on a single day

-- BritishVolt looks like an EV company, Quibi! 

-- I recognize that company - wow raised like 2 billion 

-- dollars and went under - ouch
    
-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY--------------------------------------------------------------------------------------------------
 
-- Companies with the biggest single DAY Layoff
 
SELECT company, total_laid_off

FROM layoffs_staging

ORDER BY 2 DESC

LIMIT 5;

-- now that's just on a single day
 
-- Companies with the most Total Layoffs

SELECT company, SUM(total_laid_off)

FROM layoffs_staging2

GROUP BY company

ORDER BY 2 DESC

LIMIT 10;
 
 
-- by location

SELECT location, SUM(total_laid_off)

FROM layoffs_staging2

GROUP BY location

ORDER BY 2 DESC

LIMIT 10;

-- 

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
 
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;
 
 
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
 
 -- Run the complete statement together till 435 --
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

with company_year as
(
select company,
year(date) as years,
sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(date)
),

company_year_rank as
(
select company,
years,
total_laid_off,
dense_rank() over
(
partition by years
order by total_laid_off desc
) as ranking
from company_year
)
select company, years, total_laid_off, ranking
from company_year_rank
where ranking <= 3
and years is not null;

-- Rolling total of layoffs per month
SELECT SUBSTRING(date,1,7) as dates,
SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

WITH DATE_CTE AS
(
SELECT SUBSTRING(date,1,7) as dates,
SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
)
SELECT dates,
SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
