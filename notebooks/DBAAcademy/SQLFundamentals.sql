-- Databricks notebook source
-- MAGIC %md
-- MAGIC Ensure using right database

-- COMMAND ----------

Use dbacademy_griff

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Have a look at the metadata of the people table uploaded

-- COMMAND ----------


Describe People

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run simple query against the data

-- COMMAND ----------

SELECT firstName, middleName, lastName, birthdate
FROM people
WHERE year(birthdate) > 1960 AND gender = 'F'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Have a look at some aggregations and functions in the SQL query

-- COMMAND ----------

SELECT year(birthDate) as birthYear,  firstName, count (*) AS total
FROM people
WHERE (firstName = 'Aletha' OR firstName = 'Laila') AND gender = 'F'  
  AND year(birthDate) > 1960
GROUP BY birthYear, firstName
ORDER BY birthYear, firstName

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Create a temporary view with logic baked in on

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW WomenBornAfter1990 AS
  SELECT firstName, middleName, lastName, year(birthDate) AS birthYear, salary 
  FROM people
  WHERE year(birthDate) > 1990 AND gender = 'F'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC LOok at the view for women born after 1990

-- COMMAND ----------

SELECT * FROM WomenBornAfter1990