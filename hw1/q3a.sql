--3a (5 points). Return monthly counts of pills and smooth counts of pills with a two-month window (preceding one month, following one month) (q3a.sql). 

WITH DATE_TIME AS (
    SELECT DOSAGE_UNIT AS TOTAL_DOSAGE,YEAR(TRANSACTION_DATE) ||
       CASE
           WHEN (  CAST( MONTH(TRANSACTION_DATE) AS INT)  < 10 )
               THEN '0'  || CAST( MONTH(TRANSACTION_DATE) AS INT)
           ELSE CAST ( MONTH(TRANSACTION_DATE) AS CHAR(2) )
           END AS TIME
    FROM CSE532.DEA_NY
),
TEMP AS (
    SELECT SUM(TOTAL_DOSAGE)
    AS SMOOTH1, TIME
    FROM DATE_TIME
    GROUP BY TIME
)
SELECT SMOOTH1 AS DOSAGE, AVG(SMOOTH1) OVER ( ORDER BY TIME ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS SMOOTH,TIME
FROM TEMP;