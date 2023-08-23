drop table ext_employees;
Create table ext_employees(
employee_surr_id NUMBER,
employee_first_name      VARCHAR2(50),
employee_last_name VARCHAR2(50),
 employee_email      VARCHAR2(50),
employee_phone VARCHAR2(50)
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader 
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_employees.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select * from ext_employees;