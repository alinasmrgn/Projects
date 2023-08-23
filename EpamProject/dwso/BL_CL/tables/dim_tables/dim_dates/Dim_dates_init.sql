
INSERT INTO DIM_DATES(DATE_ID)
SELECT DATE '1970-01-01' + ( LEVEL - 1 ) * INTERVAL '1' DAY AS dates
FROM   DUAL
CONNECT BY DATE '1970-01-01' + ( LEVEL - 1 ) * INTERVAL '1' DAY < DATE '2030-01-01';
COMMIT;
select * from DIM_DATES;
-- default value
Insert into DIM_DATES(DATE_ID)
Values (to_date('9999-12-31', 'yyyy-mm-dd'));




