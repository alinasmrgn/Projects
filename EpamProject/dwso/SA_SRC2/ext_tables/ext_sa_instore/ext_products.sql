drop table ext_products;
Create table ext_products(
  product_surr_id  NUMBER,
  product_brand VARCHAR2(50),
  product_line VARCHAR2(50),
  product_size VARCHAR2(50),
  product_class VARCHAR2(50)
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_products.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select * from ext_products;
