-- 📁 models/marts/dim/dim_date.sql
SELECT 
  ROW_NUMBER() OVER() AS id_date,
  d AS date
FROM UNNEST(GENERATE_DATE_ARRAY('2019-01-01', '2025-08-05')) AS d