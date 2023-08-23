drop table ext_sales_online;
CREATE TABLE ext_sales_online(
  transaction_id    NUMBER,
  customer_surr_id      NUMBER,
  delivery_address_surr_id NUMBER,
  employee_surr_id     NUMBER,
  product_surr_id   NUMBER,
  channel_surr_id      NUMBER,
  payment_type_surr_id     NUMBER,
  date_id     DATE,
  revenue   NUMBER,
  unit_price NUMBER ,
  unit_cost NUMBER,
  quantity NUMBER
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_sales_online.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;



Select *
From ext_sales_online
FETCH FIRST 20 ROWS ONLY;




