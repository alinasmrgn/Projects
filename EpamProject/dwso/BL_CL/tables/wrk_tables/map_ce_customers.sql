-- MAP_CE_CUSTOMERS;

DROP TABLE map_ce_customers;
CREATE TABLE map_ce_customers (
    customer_id                   NUMBER NULL,   
    customer_scr_id               VARCHAR2(50) NOT NULL,
    customer_scr_table            VARCHAR2(50) NOT NULL,
    customer_scr_system           VARCHAR2(50) NOT NULL,
    CUSTOMER_FIRST_NAME               VARCHAR2(50) NOT NULL,
    CUSTOMER_LAST_NAME            VARCHAR2(50) NOT NULL,
    CUSTOMER_GENDER           VARCHAR2(50) NOT NULL,
    CUSTOMER_DOB              date NOT NULL,
    CUSTOMER_WEALTH_SEGMENT            VARCHAR2(50) NOT NULL,
    CUSTOMER_JOB_ID           NUMBER NOT NULL
);
DROP SEQUENCE map_ce_customers_seq;
CREATE SEQUENCE map_ce_customers_seq
    increment by 1
    start with 0
    nomaxvalue
    minvalue 0
    nocycle
    nocache;



select * from ce_customers    
select * from map_ce_customers