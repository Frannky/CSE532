-- 2. (3 points) Write a query nearester.sql to find closest healthcare facility with an ER room
-- (AttributeValue = 'Emergency Department') from  "2799 Horseblock Road Medford, NY 11763"(40.824369, -72.993983)
-- (latitude, longitude). Please return location and distance in your result. You can use unit 'KILOMETER', 'METER',
-- or 'STATUTE MILE' for distance measurement.
--
-- Nearest neighbor search is not directed supported by DB2. You can use ST_BUFFER to create a buffered area
-- (polygon/circle) from a point within a certain distance and search only stores within the buffer. Note that 0.25
-- degree is roughly 10 miles. For all the datasets, we use spatial reference nad83_srs_1 with srs ID as 1.
-- You can find information here on functions such as ST_POINT, ST_BUFFER, ST_WITHIN or ST_CONTAINS, and ST_DISTANCE

WITH TEMP AS (SELECT A.FACILITYNAME, A.GEOLOCATION, B.AttributeValue FROM CSE532.FACILITY AS A INNER JOIN CSE532.facilitycertification AS B
              ON A.FACILITYID = B.FacilityID WHERE B.AttributeValue = 'Emergency Department')
SELECT FACILITYNAME, GEOLOCATION,
       DB2GSE.ST_DISTANCE(DB2GSE.ST_Point(40.824369, -72.993983, 1), S.GEOLOCATION, 'STATUTE MILE') AS DISTANCE_IN_MILE
FROM TEMP AS S WHERE
    DB2GSE.ST_Contains(DB2GSE.ST_BUFFER(DB2GSE.ST_Point(40.824369, -72.993983, 1), 100, 'STATUTE MILE'),  S.GEOLOCATION) = 1
ORDER BY DISTANCE_IN_MILE ASC FETCH FIRST 1 ROWS ONLY;

