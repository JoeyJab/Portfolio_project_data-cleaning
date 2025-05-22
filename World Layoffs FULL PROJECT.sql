-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove duplicates 
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove any columns


-- REMOVING DUPLICATES

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;


SELECT * 
FROM layoffs_staging2;

-- STANDARDIZING DATA

SELECT Company, TRIM(COMPANY)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT DISTINCT industry
from layoffs_staging2
ORDER BY 1;

SELECT DISTINCT industry
from layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';


-- TRIMING AND CHANGING MULTIPLE INUPUT INTO 1

SELECT DISTINCT country
FROM layoffs_staging2
order by 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country )
WHERE country LIKE 'United states%';


-- CHANGING DATE FORMAT

SELECT `DATE`
FROM layoffs_staging2;


SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';



-- FINDING NULL

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


-- UPDATING NULL

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
	
    
-- DELETING NULL
SELECT *
FROM layoffs_staging2;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



-- REMOVING COLUMNS
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;







-- EXPLORATORY DATA ANALYSIS

SELECT *
FROM layoffs_staging2;


SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- FINDING SUM, MIN, MAX, TOTAL

SELECT company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
order by 2 desc;

SELECT min(`DATE`), max(`DATE`)
FROM layoffs_staging2;

SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
order by 2 desc;

SELECT country, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY country
order by 2 desc;

SELECT year(`DATE`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY year(`DATE`)
order by 1 desc;

SELECT stage, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
order by 2 desc;


SELECT country, sum(percentage_laid_off)
FROM layoffs_staging2
GROUP BY country
order by 1 desc;


-- ROLLING TOTAL OF LAYOFFS ( MONTH, YEAR, DAY )

SELECT substring(`DATE`, 6, 2) AS MONTH, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`; 

SELECT substring(`DATE`, 1, 7) AS MONTH, sum(total_laid_off)
FROM layoffs_staging2
WHERE substring(`DATE`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC; 


WITH rolling_total AS 
(
SELECT substring(`DATE`, 1, 7) AS MONTH, sum(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`DATE`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, Total_Off
,sum(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
order by 2 desc;

SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, dense_rank() OVER (partition by years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
;
