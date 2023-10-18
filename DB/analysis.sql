-- Animals of each type with outcomes
WITH src AS (
    SELECT DISTINCT "ID", "Animal"
    FROM "ADOPTION"
    JOIN "ANIMAL" USING ("Animal_id")
)
SELECT "Animal", COUNT(*) AS total_count
FROM src
GROUP BY "Animal"
ORDER BY "Animal";

-- Count of animals with more than one outcome
WITH src AS (
    SELECT "ID", COUNT(*) AS outcome_count
    FROM "ADOPTION"
    GROUP BY "ID"
    HAVING COUNT(*) > 1
)
SELECT COUNT(*) FROM src;

-- Total Adopted cats and the count of cat types that were adopted
WITH src AS (
    SELECT DATEDIFF(YEAR, DATE(Dt), dob) AS age, "outcome_id",
        CASE WHEN age < 1 THEN 'kitten'
             WHEN age BETWEEN 1 AND 10 THEN 'Adult'
             ELSE 'Senior'
        END AS cat_type
    FROM "ADOPTION"
    JOIN "DATE" ON "ADOPTION".date_id = "DATE".date_id
    JOIN "ANIMAL" ON "ADOPTION".Animal_id = "ANIMAL".Animal_id
    WHERE "Animal" = 'Cat'
)
SELECT COUNT(*) FROM src WHERE "outcome" = 'Adopted';

-- Different cat types and their adopted count
WITH src AS (
    SELECT DATEDIFF(YEAR, DATE(Dt), dob) AS age, "outcome_id",
        CASE WHEN age < 1 THEN 'kitten'
             WHEN age BETWEEN 1 AND 10 THEN 'Adult'
             ELSE 'Senior'
        END AS cat_type
    FROM "ADOPTION"
    JOIN "DATE" ON "ADOPTION".date_id = "DATE".date_id
    JOIN "ANIMAL" ON "ADOPTION".Animal_id = "ANIMAL".Animal_id
    WHERE "Animal" = 'Cat'
)
SELECT cat_type, COUNT(*) AS adopted_count
FROM src
WHERE "outcome" = 'Adopted'
GROUP BY cat_type;

-- Cumulative Outcomes till a given date
WITH src AS (
    SELECT DATE("Dt") AS date
    FROM "ADOPTION"
    JOIN "DATE" USING (date_id)
)
SELECT DISTINCT "date", SUM(1) OVER (ORDER BY "date") AS outcomes_till_date
FROM src;
