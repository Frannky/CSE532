--2a (1 points). Create a table CSE532.ZIPPOP (ZIP, COUNTY,  GEOID, ZPOP) on populations of zip codes in NY (createzip.sql). 

CREATE TABLE CSE532.ZIPPOP(
    ZIP DECIMAL(5) NOT NULL,
    COUNTY DECIMAL(3),
    GEOID DECIMAL(10),
    ZPOP DECIMAL(10)
);