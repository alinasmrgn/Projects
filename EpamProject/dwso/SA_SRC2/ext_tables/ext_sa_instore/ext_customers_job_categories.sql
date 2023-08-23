drop table ext_customers_job_categories;
CREATE TABLE ext_customers_job_categories(
    customer_job_category_id      NUMBER,
  customer_job_industry_category VARCHAR2(50)
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_cust_job_cat.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select *
From ext_customers_job_categories;
