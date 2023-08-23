CREATE OR REPLACE PACKAGE BODY pkg_etl_load
IS

    PROCEDURE etl_init_load
    AS
   obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
        
         log_writer('MSG', obj_name, sysdate, null, 'Start init loading into sa');
        
        pkg_etl_sa_load.ld_sa_payment_types;
        pkg_etl_sa_load.ld_sa_channels;
        pkg_etl_sa_load.ld_sa_delivery_address;
        pkg_etl_sa_load.ld_sa_employees;
        pkg_etl_sa_load.ld_sa_products;
        pkg_etl_sa_load.ld_sa_customer_jobs;
        pkg_etl_sa_load.ld_sa_customer_job_categories;
        pkg_etl_sa_load.ld_sa_customer_instore;
        pkg_etl_sa_load.ld_sa_customer_online;
        
       log_writer('MSG', obj_name, sysdate, null,  'Start init loading into bl_3nf');
        
        pkg_etl_ce_products.etl_ce_product_brands;
        pkg_etl_ce_products.etl_ce_products;
        pkg_etl_ce_payment_types.etl_ce_payment_types;
        pkg_etl_ce_employees_scd.etl_wrk_ce_employees_scd;
        pkg_etl_ce_employees_scd.etl_ce_employees_scd_merge_step1;
        pkg_etl_ce_employees_scd.etl_ce_employees_scd_insert_step2;
        pkg_etl_ce_delivery_addresses.etl_ce_delivery_states;
        pkg_etl_ce_customers.etl_ce_customer_job_categories;
        pkg_etl_ce_customers.etl_ce_customer_jobs;
        pkg_etl_ce_customers.etl_map_ce_customers_instore;
        pkg_etl_ce_customers.etl_map_ce_customers_online;
        pkg_etl_ce_customers.etl_ce_customers;
        pkg_etl_ce_channels.etl_ce_channels_classes;
        pkg_etl_ce_channels.etl_ce_channels;
        pkg_etl_fct_sales_init.ld_wrk_sales_init;
        pkg_etl_fct_sales_init.etl_ce_sales_init;
        
         log_writer('MSG', obj_name, sysdate, null,  'Start init loading into bl_dm');
        
        pkg_etl_dim_products.etl_dim_products;
        --pkg_etl_dim_employees_scd.etl_dim_employees_scd;
        pkg_etl_dim_customers.etl_dim_customers;
        pkg_etl_dim_channels.etl_dim_channels;
        pkg_etl_dim_payment_types.etl_dim_payment_types;
        pkg_etl_dim_delivery_addresses.etl_dim_delivery_addresses;        
        pkg_etl_fct_sales_init.etl_fct_sales_init;

        log_writer('MSG', obj_name, sysdate, null,  'Finish init loading');
        
    END etl_init_load;
    
    PROCEDURE etl_reload (start_reload_dt VARCHAR2, end_reload_dt VARCHAR2)
    AS
   obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
        
         log_writer('MSG', obj_name, sysdate, null, 'Start reloading into sa');
        
        pkg_etl_sa_load.ld_sa_payment_types;
        pkg_etl_sa_load.ld_sa_channels;
        pkg_etl_sa_load.ld_sa_delivery_address;
        pkg_etl_sa_load.ld_sa_employees;
        pkg_etl_sa_load.ld_sa_products;
        pkg_etl_sa_load.ld_sa_customer_jobs;
        pkg_etl_sa_load.ld_sa_customer_job_categories;
        pkg_etl_sa_load.ld_sa_customer_instore;
        pkg_etl_sa_load.ld_sa_customer_online;
        
        
        log_writer('MSG', obj_name, sysdate, null, 'Start reloading into bl_3nf');
        
        pkg_etl_ce_products.etl_ce_product_brands;
        pkg_etl_ce_products.etl_ce_products;
        pkg_etl_ce_payment_types.etl_ce_payment_types;
        pkg_etl_ce_employees_scd.etl_wrk_ce_employees_scd;
        pkg_etl_ce_employees_scd.etl_ce_employees_scd_merge_step1;
        pkg_etl_ce_employees_scd.etl_ce_employees_scd_insert_step2;
        pkg_etl_ce_delivery_addresses.etl_ce_delivery_states;
        pkg_etl_ce_customers.etl_ce_customer_job_categories;
        pkg_etl_ce_customers.etl_ce_customer_jobs;
        pkg_etl_ce_customers.etl_map_ce_customers_instore;
        pkg_etl_ce_customers.etl_map_ce_customers_online;
        pkg_etl_ce_customers.etl_ce_customers;
        pkg_etl_ce_channels.etl_ce_channels_classes;
        pkg_etl_ce_channels.etl_ce_channels;
        pkg_etl_fct_sales.ld_wrk_sales;
        pkg_etl_fct_sales.etl_ce_sales;
        
       log_writer('MSG', obj_name, sysdate, null, 'Start reloading into bl_dm');
        
        pkg_etl_dim_products.etl_dim_products;
        --pkg_etl_dim_employees_scd.etl_dim_employees_scd;
        pkg_etl_dim_customers.etl_dim_customers;
        pkg_etl_dim_channels.etl_dim_channels;
        pkg_etl_dim_payment_types.etl_dim_payment_types;
        pkg_etl_dim_delivery_addresses.etl_dim_delivery_addresses;        
        pkg_etl_fct_sales_init.etl_fct_sales_init;     
        pkg_etl_fct_sales.etl_fct_sales(start_reload_dt,end_reload_dt);

        log_writer('MSG', obj_name, sysdate, null, 'Finish reloading');    
        
    END etl_reload;        
END pkg_etl_load;        
 exec pkg_etl_load.etl_reload('01-01-19', '01-10-20');       