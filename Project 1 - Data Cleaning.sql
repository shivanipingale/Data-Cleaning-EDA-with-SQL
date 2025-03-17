----  Data Cleaning 

Select * from world_layoffs.layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns and rows

create table layoffs_staging
like layoffs;

select * from layoffs_staging;


insert into layoffs_staging
select * from layoffs;

select *, row_number() over(partition By company) As row_num
from layoffs_staging;
-- create a table where new or filter data is added

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` Int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;
insert into layoffs_staging2
select *, row_number() over(partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) As row_num from layoffs_staging;

select * from layoffs_staging2
where row_num > 1;
Delete from layoffs_staging2
where row_num = 2;

--- stadardizing data - finding issues in data

select company, (Trim(company)) 
from layoffs_staging2;

update layoffs_staging2 set company = Trim(company);

select distinct industry from layoffs_staging2
order by 1;

select * from layoffs_staging2 where industry like 'crypto%';
select Count(*) from layoffs_staging2;
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypt%';

select * from layoffs_staging2
where row_num = 2 order by 1;
select distinct country, (trim(country)) from layoffs_staging2;
update layoffs_staging2 set country = trim(country);

select country from layoffs_staging2 where country = 'united states.';
update layoffs_staging2 set country = 'united states' where country = 'united states.';

Select date from layoffs_staging2;
select date, str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;  ---- date in format YYYY-MM-DD
update layoffs_staging2 set `date` = str_to_date(`date`, '%m/%d/%Y');

select date from layoffs_staging2;

Alter table layoffs_staging2 
modify column `date` Date; 

select * from layoffs_staging2
where industry is null
OR industry = '';

select * from layoffs_staging2 where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1 
join layoffs_staging2  t2
    on t1. Company = t2. Company
where (t1.industry is null or t1. industry = '') 
and t2. industry is not null;

Update layoffs_staging2 set industry = null where industry ='';

update layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.Company = t2.Company
set t1.industry = t2.industry 
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

Delete from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

select * from layoffs_staging2;

Alter table layoffs_staging2
drop column row_num;

-- Exploratory DATA Analysis

select * from layoffs_staging2
where (total_laid_off) = 12000;

select * from layoffs_staging2 where percentage_laid_off = 1
order by total_laid_off desc;

select * from layoffs_staging2 
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select Min(`date`), Max(`date`) 
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select Year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select * from layoffs_staging2;

select year(`date`), sum(total_laid_off)
from layoffs_staging2 as Total_laid_off
group by year(`date`)
order by 1;


With Month_total_off AS
(
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2 where substring(`date`, 1, 7) is not null
group by month
order by 1 asc
),
Company_Year (company, years, total_laid_off) AS
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc
), Company_years_rank As
(
Select *, dense_rank() over (partition by years order by total_laid_off Desc) As Ranking
from company_Year 
where years is not null
)
select month, total_off, sum(total_off) over (order by month) as rolling_total
from Month_total_off;
Select * from company_years_rank where ranking <= 5
order by total_laid_off desc;

Select * from layoffs_staging2;















