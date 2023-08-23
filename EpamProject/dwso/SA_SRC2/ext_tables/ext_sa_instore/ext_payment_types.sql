drop table ext_payment_types;
Create table ext_payment_types(
    payment_type_surr_id NUMBER,
    payment_type_name  VARCHAR2(50)
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_payment_types.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select * from ext_payment_types;
