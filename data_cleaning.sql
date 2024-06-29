-- From the dataset, I have tried 4 things to clean the data
-- 1. check for duplicate rows and remove if any
-- 2. standardizing the data
-- 3. checking null and empty values
-- 4. removing rows/columns that are not necessary

SELECT * FROM a.layoffs;

-- 1. remove duplicates if any
-- checked what are the duplicates
with duplicate_ctes as 
(select *, Row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num from a.layoffs)
select * from duplicate_ctes where row_num > 1;

-- delete the duplicated by creating and new table and then deleting, so that if any mistake is made the raw data is still available

CREATE TABLE a.`layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into a.layoffs2
select *, Row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num from a.layoffs;

select * from a.layoffs2;
SET SQL_SAFE_UPDATES = 0;
delete from a.layoffs2 where row_num > 1;

-- standardizing the data
update a.layoffs2 set company = trim(company);

-- checking the industry
select distinct(industry) from a.layoffs2 order by 1;

-- Industry like Crypto had different names like crypto currency, crypto etc. Updated all of it to just Crypto.
select * from a.layoffs2 where industry like 'Crypto%';
update a.layoffs2 set industry = 'Crypto' where industry like 'Crypto%';

-- checking all the other columns to see if there are any discrepancies
select distinct(location) from a.layoffs2 order by 1;
select distinct(country) from a.layoffs2 order by 1;

-- United states appeared twice as a different country because one of it had a period in the end. 
update a.layoffs2 set country = 'United States' where country like 'United States%';

-- changed the date to an understandable format and altered it from a text to a date field
update a.layoffs2 set `date`= str_to_date(`date`, '%m/%d/%Y') ;
Alter table a.layoffs2 modify column `date` DATE;

-- null and blank values
-- the industry was null in one place but had 'travel'in another row, so whereever company is airbnb we can populate it with travel
-- we can do that for other companies as well by joining on itself.
select * from a.layoffs2 where industry is null or industry = '';
select * from a.layoffs2 where company = 'Airbnb';

select * from a.layoffs2 t1 join a.layoffs2 t2 on 
t1.company = t2.company and t1.location = t2.location where (t1.industry is null or t1.industry = '') and t2.industry is not null;

UPDATE a.layoffs2 t1 JOIN a.layoffs2 t2
ON t1.company = t2.company SET t1.industry = t2.industry WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- if both the total_laid_off and percentage_laid_off is null, then it might not be required for any analysis as it does not give any meaningful information
-- so delete those rows
select * from a.layoffs2 where total_laid_off is NULL and percentage_laid_off is NULL;

delete from a.layoffs2 where total_laid_off is NULL and percentage_laid_off is NULL;

-- remove the row_num column as it is no longer required to check duplicates as they have been removed
alter table a.layoffs2 drop column row_num;

-- final cleaned database
select * from a.layoffs2;
