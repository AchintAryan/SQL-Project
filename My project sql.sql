-- Data Cleaning

SELECT*
from layoffs;



CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT*
FROM layoffs_staging;

INSERT layoffs_staging
SELECT*
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, country, stage,  funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

SELECT*
FROM layoffs_staging2
WHERE company = 'Casper';

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT*
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, country, stage
, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT*
FROM layoffs_staging2
WHERE row_num > 1;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT*
FROM layoffs_staging2;

SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM layoffs_staging2;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, country, stage,  funds_raised_millions) AS row_num
FROM layoffs_staging2
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT*
FROM layoffs_staging3;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, country, stage,  funds_raised_millions) AS row_num
FROM layoffs_staging2;

SELECT*
FROM layoffs_staging3;

DELETE
FROM layoffs_staging3
WHERE row_num = 3;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;


-- Standardizing the data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT*
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry  = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry ='';


SELECT*
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT*
FROM layoffs_staging3
WHERE company LIKE 'Bally%';

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
     
SELECT*
FROM layoffs_staging2;
     
SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM layoffs_staging2;


-- Exploratory Data Analysis

SELECT*
FROM layoffs_staging3;

 
SELECT MAX(total_laid_off), MAX(Percentage_laid_off)
FROM layoffs_staging3;

SELECT*
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company
ORDER BY 2 DESC;


SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging3;


SELECT industry, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY country
ORDER BY 2 DESC;


SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY stage
ORDER BY 2 DESC;


SELECT SUBSTRING(`date`,1,7)AS `MONTH`, SUM(total_laid_off) AS Total_off
FROM layoffs_staging3
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC;

WITH ROlling_Total AS 
(
 SELECT SUBSTRING(`date`,1,7)AS MONTH, SUM(total_laid_off)AS Total_off
 FROM layoffs_staging3
 WHERE SUBSTRING(`date`,1,7) IS NOT NULL
 GROUP BY MONTH
 ORDER BY 1 ASC
 )
 SELECT `MONTH`, total_off,
 SUM(Total_off) OVER(ORDER BY `MONTH`) AS rolling_total
 FROM Rolling_Total;
 
 
 SELECT company, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company
ORDER BY 2 DESC;


SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year (Company,Years,Total_Laid_Off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company, YEAR(`date`)
),Company_Year_Rank AS(
SELECT* ,
 DENSE_RANK() OVER (PARTITION BY Years ORDER BY Total_Laid_Off DESC) AS Ranking
FROM Company_Year
WHERE YEARS IS NOT NULL
)

SELECT*
FROM Company_Year_Rank
WHERE Ranking <=5





















