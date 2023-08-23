CREATE OR REPLACE PACKAGE pkg_etl_sa_load
AS
    PROCEDURE ld_sa_payment_types;
    PROCEDURE ld_sa_channels;
    PROCEDURE ld_sa_delivery_address;
    PROCEDURE ld_sa_employees;
    PROCEDURE ld_sa_products;
    PROCEDURE ld_sa_customer_jobs;
    PROCEDURE ld_sa_customer_job_categories;
    PROCEDURE ld_sa_customer_instore;
    PROCEDURE ld_sa_customer_online;
END pkg_etl_sa_load;
