drop table ext_delivery_address;
Create table ext_delivery_address(
delivery_address_surr_id NUMBER,
 delivery_street_address      VARCHAR2(50),
  delivery_postal_code      NUMBER,
  delivery_state_id NUMBER,
  delivery_state     VARCHAR2(50),
   delivery_country_id NUMBER,
  delivery_country      VARCHAR2(50)
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_delivery_address.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select * from ext_delivery_address;