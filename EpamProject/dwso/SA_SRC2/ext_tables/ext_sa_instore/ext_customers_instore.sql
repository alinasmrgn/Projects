drop table ext_customers_instore;
Create table ext_customers_instore(
customer_surr_id NUMBER,
customer_first_name      VARCHAR2(50),
  customer_last_name VARCHAR2(50),
  customer_gender     VARCHAR2(50),
  customer_dob    date,
  customer_job_title      VARCHAR2(50),
  customer_job_industry_category      VARCHAR2(50),
  customer_wealth_segment      VARCHAR2(50),
  owns_car      VARCHAR2(50)

)
ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_customer_instore.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select * from ext_customers_instore;