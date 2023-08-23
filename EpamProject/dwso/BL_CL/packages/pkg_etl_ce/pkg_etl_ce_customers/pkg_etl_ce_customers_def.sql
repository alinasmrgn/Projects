CREATE OR REPLACE PACKAGE pkg_etl_ce_customers
AS  
    PROCEDURE etl_ce_customers;
    PROCEDURE etl_map_ce_customers_instore;
    PROCEDURE etl_map_ce_customers_online; 
    PROCEDURE etl_ce_customer_jobs;   
    PROCEDURE etl_ce_customer_job_categories;   

END pkg_etl_ce_customers; 