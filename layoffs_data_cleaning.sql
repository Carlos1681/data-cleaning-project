 -- Limpeza de Dados --
 
 SELECT * FROM layoffs;
 
 -- Remoção de dados duplicados --
 -- Adequar os dados, como remover espaços e etc --
 -- Buscar por valores nulos ou em branco --
 -- Remoção de colunas ou linhas não importantes --
 
 -- Para não modificar a tabela original, irei utilizar cópias --
 CREATE TABLE layoffs_staging
 LIKE layoffs;
 
 INSERT layoffs_staging
 SELECT *
 FROM layoffs;
 
 SELECT * FROM layoffs_staging;
 
 -- Remoção de dados duplicados --
 
 WITH duplicate_cte AS
 (
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location ,industry, total_laid_off,
 percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 )
 SELECT * FROM duplicate_cte
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location ,industry, total_laid_off,
 percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging;
 
 DELETE FROM layoffs_staging2
 WHERE row_num > 1;
 
 SELECT * FROM layoffs_staging2
 WHERE row_num > 2;
 
-- Adequar os dados, como remover espaços e etc --
 
 SELECT company, TRIM(company)
 from layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET company = TRIM(company);
 
 SELECT *
 from layoffs_staging2
 WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
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
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Buscar por valores nulos ou em branco --

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT l1.industry, l2.industry
FROM layoffs_staging2 l1
JOIN layoffs_staging2 l2
ON l1.company = l2.company
WHERE l1.industry IS NULL
AND l2.industry IS NOT NULL;

UPDATE layoffs_staging2 l1
JOIN layoffs_staging2 l2
ON l1.company = l2.company
SET l1.industry = l2.industry
WHERE l1.industry IS NULL
AND l2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

 -- Remoção de colunas ou linhas não importantes --

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
 
