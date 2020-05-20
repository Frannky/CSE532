--1c Based on queries in 3, create proper indexes to make the queries more efficient

CREATE INDEX TRANSACTION_DATE_INDEX ON CSE532.DEA_NY(TRANSACTION_DATE);
CREATE INDEX BUYER_ZIP_INDEX ON CSE532.DEA_NY(BUYER_ZIP);