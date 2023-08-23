CREATE OR REPLACE PACKAGE BODY pkg_etl_sa_load
IS
   
    
    PROCEDURE ld_sa_payment_types
    AS
   obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        MERGE INTO sa_payment_types trg
        USING(  SELECT
                    PAYMENT_TYPE_SURR_ID,
                    PAYMENT_TYPE_NAME
                FROM sa_instore.ext_payment_types) src
        ON (  trg.PAYMENT_TYPE_SURR_ID = src.PAYMENT_TYPE_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    trg.PAYMENT_TYPE_NAME = src.PAYMENT_TYPE_NAME
            WHERE DECODE(src.PAYMENT_TYPE_SURR_ID,trg.PAYMENT_TYPE_SURR_ID,0,1)
                + DECODE(src.PAYMENT_TYPE_NAME,trg.PAYMENT_TYPE_NAME,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.PAYMENT_TYPE_SURR_ID,
                trg.PAYMENT_TYPE_NAME)
            VALUES(
                src.PAYMENT_TYPE_SURR_ID,
                src.PAYMENT_TYPE_NAME);
         log_writer('MSG', obj_name, sysdate, null,  'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null,  'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_payment_types;    
 -----------------------------------------------------------------------------------------
    PROCEDURE ld_sa_channels
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        MERGE INTO sa_channels trg
        USING(  SELECT
                    CHANNEL_SURR_ID,
                    CHANNEL_NAME,
                    CHANNEL_CLASS_ID,
                    CHANNEL_CLASS
                FROM sa_instore.ext_channels) src
        ON (src.CHANNEL_SURR_ID = trg.CHANNEL_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    trg.CHANNEL_NAME = src.CHANNEL_NAME,
                    trg.CHANNEL_CLASS_ID = src.CHANNEL_CLASS_ID,
                    trg.CHANNEL_CLASS = src.CHANNEL_CLASS
            WHERE DECODE(src.CHANNEL_SURR_ID,trg.CHANNEL_SURR_ID,0,1)
                + DECODE(src.CHANNEL_NAME,trg.CHANNEL_NAME,0,1)
                + DECODE(src.CHANNEL_CLASS_ID,trg.CHANNEL_CLASS_ID,0,1)
                + DECODE(src.CHANNEL_CLASS,trg.CHANNEL_CLASS,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.CHANNEL_SURR_ID,
                trg.CHANNEL_NAME,
                trg.CHANNEL_CLASS_ID,
                trg.CHANNEL_CLASS)
            VALUES(
                src.CHANNEL_SURR_ID,
                src.CHANNEL_NAME,
                src.CHANNEL_CLASS_ID,
                src.CHANNEL_CLASS);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_channels;
 -----------------------------------------------------------------------------------------------------   
    PROCEDURE ld_sa_delivery_address
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_delivery_address trg
        USING(  SELECT
                    DELIVERY_ADDRESS_SURR_ID,
                    DELIVERY_STREET_ADDRESS,
                    DELIVERY_POSTAL_CODE,
                    DELIVERY_STATE_ID,
                    DELIVERY_STATE,
                    DELIVERY_COUNTRY
                FROM sa_instore.ext_delivery_address) src
        ON (src.DELIVERY_ADDRESS_SURR_ID = trg.DELIVERY_ADDRESS_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    
                    trg.DELIVERY_STREET_ADDRESS = src.DELIVERY_STREET_ADDRESS,
                    trg.DELIVERY_POSTAL_CODE = src.DELIVERY_POSTAL_CODE,
                    trg.DELIVERY_STATE_ID = src.DELIVERY_STATE_ID,
                    trg.DELIVERY_STATE = src.DELIVERY_STATE,
                    trg.DELIVERY_COUNTRY = src.DELIVERY_COUNTRY
            WHERE DECODE(src.DELIVERY_ADDRESS_SURR_ID,trg.DELIVERY_ADDRESS_SURR_ID,0,1)
                + DECODE(src.DELIVERY_STREET_ADDRESS,trg.DELIVERY_STREET_ADDRESS,0,1)
                + DECODE(src.DELIVERY_POSTAL_CODE,trg.DELIVERY_POSTAL_CODE,0,1)
                + DECODE(src.DELIVERY_STATE_ID,trg.DELIVERY_STATE_ID,0,1)
                + DECODE(src.DELIVERY_STATE,trg.DELIVERY_STATE,0,1)
                + DECODE(src.DELIVERY_COUNTRY,trg.DELIVERY_COUNTRY,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.DELIVERY_ADDRESS_SURR_ID,
                trg.DELIVERY_STREET_ADDRESS,
                trg.DELIVERY_POSTAL_CODE,
                trg.DELIVERY_STATE_ID,
                trg.DELIVERY_STATE,
                trg.DELIVERY_COUNTRY)
            VALUES(
                src.DELIVERY_ADDRESS_SURR_ID,
                src.DELIVERY_STREET_ADDRESS,
                src.DELIVERY_POSTAL_CODE,
                src.DELIVERY_STATE_ID,
                src.DELIVERY_STATE,
                src.DELIVERY_COUNTRY);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_delivery_address;    
 --------------------------------------------------------------------------------   
      PROCEDURE ld_sa_employees
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_employees trg
        USING(  SELECT
                    EMPLOYEE_SURR_ID,
                    EMPLOYEE_FIRST_NAME,
                    EMPLOYEE_LAST_NAME,
                    EMPLOYEE_EMAIL,
                    EMPLOYEE_PHONE
                FROM sa_instore.ext_employees) src
        ON (src.EMPLOYEE_SURR_ID = trg.EMPLOYEE_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                   
                    trg.EMPLOYEE_FIRST_NAME = src.EMPLOYEE_FIRST_NAME,
                    trg.EMPLOYEE_LAST_NAME = src.EMPLOYEE_LAST_NAME,
                    trg.EMPLOYEE_EMAIL = src.EMPLOYEE_EMAIL,
                    trg.EMPLOYEE_PHONE = src.EMPLOYEE_PHONE
            WHERE DECODE(src.EMPLOYEE_SURR_ID,trg.EMPLOYEE_SURR_ID,0,1)
                + DECODE(src.EMPLOYEE_FIRST_NAME,trg.EMPLOYEE_FIRST_NAME,0,1)
                + DECODE(src.EMPLOYEE_LAST_NAME,trg.EMPLOYEE_LAST_NAME,0,1)
                + DECODE(src.EMPLOYEE_EMAIL,trg.EMPLOYEE_EMAIL,0,1)
                + DECODE(src.EMPLOYEE_PHONE,trg.EMPLOYEE_PHONE,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.EMPLOYEE_SURR_ID,
                trg.EMPLOYEE_FIRST_NAME,
                trg.EMPLOYEE_LAST_NAME,
                trg.EMPLOYEE_EMAIL,
                trg.EMPLOYEE_PHONE)
            VALUES(
                src.EMPLOYEE_SURR_ID,
                src.EMPLOYEE_FIRST_NAME,
                src.EMPLOYEE_LAST_NAME,
                src.EMPLOYEE_EMAIL,
                src.EMPLOYEE_PHONE);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_employees;
    ------------------------------------------------------------------------------------------------------
   PROCEDURE ld_sa_products
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_products trg
        USING(  SELECT
                    PRODUCT_SURR_ID,
                    PRODUCT_BRAND_ID,
                    PRODUCT_BRAND,
                    PRODUCT_LINE,
                    PRODUCT_SIZE,
                    PRODUCT_CLASS
                FROM sa_instore.ext_products) src
        ON (src.PRODUCT_SURR_ID = trg.PRODUCT_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    
                    trg.PRODUCT_BRAND_ID = src.PRODUCT_BRAND_ID,
                    trg.PRODUCT_BRAND = src.PRODUCT_BRAND,
                    trg.PRODUCT_LINE = src.PRODUCT_LINE,
                    trg.PRODUCT_SIZE = src.PRODUCT_SIZE,
                    trg.PRODUCT_CLASS = src.PRODUCT_CLASS
            WHERE DECODE(src.PRODUCT_SURR_ID,trg.PRODUCT_SURR_ID,0,1)
                + DECODE(src.PRODUCT_BRAND_ID,trg.PRODUCT_BRAND_ID,0,1)
                + DECODE(src.PRODUCT_BRAND,trg.PRODUCT_BRAND,0,1)
                + DECODE(src.PRODUCT_LINE,trg.PRODUCT_LINE,0,1)
                + DECODE(src.PRODUCT_SIZE,trg.PRODUCT_SIZE,0,1)
                + DECODE(src.PRODUCT_CLASS,trg.PRODUCT_CLASS,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.PRODUCT_SURR_ID,
                trg.PRODUCT_BRAND_ID,
                trg.PRODUCT_BRAND,
                trg.PRODUCT_LINE,
                trg.PRODUCT_SIZE,
                trg.PRODUCT_CLASS)
            VALUES(
                src.PRODUCT_SURR_ID,
                src.PRODUCT_BRAND_ID,
                src.PRODUCT_BRAND,
                src.PRODUCT_LINE,
                src.PRODUCT_SIZE,
                src.PRODUCT_CLASS);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_products;    
-------------------------------------------------------------------------------------------------
   PROCEDURE ld_sa_customer_jobs
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_customer_jobs trg
        USING(  SELECT
                    CUSTOMER_JOB_ID,
                    CUSTOMER_JOB_TITLE,
                    CUSTOMER_JOB_CATEGORY_ID,
                    CUSTOMER_JOB_INDUSTRY_CATEGORY
                FROM sa_instore.ext_customers_jobs) src
        ON (src.CUSTOMER_JOB_ID = trg.CUSTOMER_JOB_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    
                    trg.CUSTOMER_JOB_TITLE = src.CUSTOMER_JOB_TITLE,
                    trg.CUSTOMER_JOB_CATEGORY_ID = src.CUSTOMER_JOB_CATEGORY_ID,
                    trg.CUSTOMER_JOB_INDUSTRY_CATEGORY = src.CUSTOMER_JOB_INDUSTRY_CATEGORY
            WHERE DECODE(src.CUSTOMER_JOB_ID,trg.CUSTOMER_JOB_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_TITLE,trg.CUSTOMER_JOB_TITLE,0,1)
                + DECODE(src.CUSTOMER_JOB_CATEGORY_ID,trg.CUSTOMER_JOB_CATEGORY_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_INDUSTRY_CATEGORY,trg.CUSTOMER_JOB_INDUSTRY_CATEGORY,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.CUSTOMER_JOB_ID,
                trg.CUSTOMER_JOB_TITLE,
                trg.CUSTOMER_JOB_CATEGORY_ID,
                trg.CUSTOMER_JOB_INDUSTRY_CATEGORY)
            VALUES(
                src.CUSTOMER_JOB_ID,
                src.CUSTOMER_JOB_TITLE,
                src.CUSTOMER_JOB_CATEGORY_ID,
                src.CUSTOMER_JOB_INDUSTRY_CATEGORY);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_customer_jobs;    
-------------------------------------------------------------------------------------------------
  PROCEDURE ld_sa_customer_job_categories
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_customer_job_categories trg
        USING(  SELECT
                    CUSTOMER_JOB_CATEGORY_ID,
                    CUSTOMER_JOB_INDUSTRY_CATEGORY
                FROM sa_instore.ext_customers_job_categories) src
        ON (src.CUSTOMER_JOB_CATEGORY_ID = trg.CUSTOMER_JOB_CATEGORY_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                   
                    trg.CUSTOMER_JOB_INDUSTRY_CATEGORY = src.CUSTOMER_JOB_INDUSTRY_CATEGORY
            WHERE DECODE(src.CUSTOMER_JOB_CATEGORY_ID,trg.CUSTOMER_JOB_CATEGORY_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_INDUSTRY_CATEGORY,trg.CUSTOMER_JOB_INDUSTRY_CATEGORY,0,1) > 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.CUSTOMER_JOB_CATEGORY_ID,
                trg.CUSTOMER_JOB_INDUSTRY_CATEGORY)
            VALUES(
                src.CUSTOMER_JOB_CATEGORY_ID,
                src.CUSTOMER_JOB_INDUSTRY_CATEGORY);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_customer_job_categories;    
-------------------------------------------------------------------------------------------------
 PROCEDURE ld_sa_customer_instore
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_customer_instore trg
        USING(  SELECT
                    CUSTOMER_SURR_ID,
                    CUSTOMER_FIRST_NAME,
                    CUSTOMER_LAST_NAME,
                    CUSTOMER_GENDER,
                    CUSTOMER_DOB,
                    CUSTOMER_JOB_ID,
                    CUSTOMER_JOB_TITLE,
                    CUSTOMER_JOB_CATEGORY_ID,
                    CUSTOMER_JOB_INDUSTRY_CATEGORY,
                    CUSTOMER_WEALTH_SEGMENT
                FROM sa_instore.ext_customers_instore) src
        ON (src.CUSTOMER_SURR_ID = trg.CUSTOMER_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    
                    trg.CUSTOMER_FIRST_NAME = src.CUSTOMER_FIRST_NAME,
                    trg.CUSTOMER_LAST_NAME = src.CUSTOMER_LAST_NAME,
                    trg.CUSTOMER_GENDER = src.CUSTOMER_GENDER,
                    trg.CUSTOMER_DOB = src.CUSTOMER_DOB,
                    trg.CUSTOMER_JOB_ID = src.CUSTOMER_JOB_ID,
                    trg.CUSTOMER_JOB_TITLE = src.CUSTOMER_JOB_TITLE,
                    trg.CUSTOMER_JOB_CATEGORY_ID = src.CUSTOMER_JOB_CATEGORY_ID,
                    trg.CUSTOMER_JOB_INDUSTRY_CATEGORY = src.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                    trg.CUSTOMER_WEALTH_SEGMENT = src.CUSTOMER_WEALTH_SEGMENT
                    
            WHERE DECODE(src.CUSTOMER_SURR_ID,trg.CUSTOMER_SURR_ID,0,1)
                + DECODE(src.CUSTOMER_FIRST_NAME,trg.CUSTOMER_FIRST_NAME,0,1)
                + DECODE(src.CUSTOMER_LAST_NAME,trg.CUSTOMER_LAST_NAME,0,1)
                + DECODE(src.CUSTOMER_GENDER,trg.CUSTOMER_GENDER,0,1)
                + DECODE(src.CUSTOMER_DOB,trg.CUSTOMER_DOB,0,1)
                + DECODE(src.CUSTOMER_JOB_ID,trg.CUSTOMER_JOB_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_TITLE,trg.CUSTOMER_JOB_TITLE,0,1)
                + DECODE(src.CUSTOMER_JOB_CATEGORY_ID,trg.CUSTOMER_JOB_CATEGORY_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_INDUSTRY_CATEGORY,trg.CUSTOMER_JOB_INDUSTRY_CATEGORY,0,1)
                + DECODE(src.CUSTOMER_WEALTH_SEGMENT,trg.CUSTOMER_WEALTH_SEGMENT,0,1)> 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.CUSTOMER_SURR_ID,
                trg.CUSTOMER_FIRST_NAME,
                trg.CUSTOMER_LAST_NAME,
                trg.CUSTOMER_GENDER,
                trg.CUSTOMER_DOB,
                trg.CUSTOMER_JOB_ID,
                trg.CUSTOMER_JOB_TITLE,
                trg.CUSTOMER_JOB_CATEGORY_ID,
                trg.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                trg.CUSTOMER_WEALTH_SEGMENT)
            VALUES(
                src.CUSTOMER_SURR_ID,
                src.CUSTOMER_FIRST_NAME,
                src.CUSTOMER_LAST_NAME,
                src.CUSTOMER_GENDER,
                src.CUSTOMER_DOB,
                src.CUSTOMER_JOB_ID,
                src.CUSTOMER_JOB_TITLE,
                src.CUSTOMER_JOB_CATEGORY_ID,
                src.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                src.CUSTOMER_WEALTH_SEGMENT);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_customer_instore;    
-------------------------------------------------------------------------------------------------
 PROCEDURE ld_sa_customer_online
    AS
     obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge');
        
        
        MERGE INTO sa_customer_online trg
        USING(  SELECT
                    CUSTOMER_SURR_ID,
                    CUSTOMER_FIRST_NAME,
                    CUSTOMER_LAST_NAME,
                    CUSTOMER_GENDER,
                    CUSTOMER_DOB,
                    CUSTOMER_JOB_ID,
                    CUSTOMER_JOB_TITLE,
                    CUSTOMER_JOB_CATEGORY_ID,
                    CUSTOMER_JOB_INDUSTRY_CATEGORY,
                    CUSTOMER_WEALTH_SEGMENT
                FROM sa_online.ext_customers_online) src
        ON (src.CUSTOMER_SURR_ID = trg.CUSTOMER_SURR_ID)    
        WHEN MATCHED THEN
            UPDATE SET  
                    
                    trg.CUSTOMER_FIRST_NAME = src.CUSTOMER_FIRST_NAME,
                    trg.CUSTOMER_LAST_NAME = src.CUSTOMER_LAST_NAME,
                    trg.CUSTOMER_GENDER = src.CUSTOMER_GENDER,
                    trg.CUSTOMER_DOB = src.CUSTOMER_DOB,
                    trg.CUSTOMER_JOB_ID = src.CUSTOMER_JOB_ID,
                    trg.CUSTOMER_JOB_TITLE = src.CUSTOMER_JOB_TITLE,
                    trg.CUSTOMER_JOB_CATEGORY_ID = src.CUSTOMER_JOB_CATEGORY_ID,
                    trg.CUSTOMER_JOB_INDUSTRY_CATEGORY = src.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                    trg.CUSTOMER_WEALTH_SEGMENT = src.CUSTOMER_WEALTH_SEGMENT
                    
            WHERE DECODE(src.CUSTOMER_SURR_ID,trg.CUSTOMER_SURR_ID,0,1)
                + DECODE(src.CUSTOMER_FIRST_NAME,trg.CUSTOMER_FIRST_NAME,0,1)
                + DECODE(src.CUSTOMER_LAST_NAME,trg.CUSTOMER_LAST_NAME,0,1)
                + DECODE(src.CUSTOMER_GENDER,trg.CUSTOMER_GENDER,0,1)
                + DECODE(src.CUSTOMER_DOB,trg.CUSTOMER_DOB,0,1)
                + DECODE(src.CUSTOMER_JOB_ID,trg.CUSTOMER_JOB_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_TITLE,trg.CUSTOMER_JOB_TITLE,0,1)
                + DECODE(src.CUSTOMER_JOB_CATEGORY_ID,trg.CUSTOMER_JOB_CATEGORY_ID,0,1)
                + DECODE(src.CUSTOMER_JOB_INDUSTRY_CATEGORY,trg.CUSTOMER_JOB_INDUSTRY_CATEGORY,0,1)
                + DECODE(src.CUSTOMER_WEALTH_SEGMENT,trg.CUSTOMER_WEALTH_SEGMENT,0,1)> 0
        WHEN NOT MATCHED THEN
            INSERT(
                trg.CUSTOMER_SURR_ID,
                trg.CUSTOMER_FIRST_NAME,
                trg.CUSTOMER_LAST_NAME,
                trg.CUSTOMER_GENDER,
                trg.CUSTOMER_DOB,
                trg.CUSTOMER_JOB_ID,
                trg.CUSTOMER_JOB_TITLE,
                trg.CUSTOMER_JOB_CATEGORY_ID,
                trg.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                trg.CUSTOMER_WEALTH_SEGMENT)
            VALUES(
                src.CUSTOMER_SURR_ID,
                src.CUSTOMER_FIRST_NAME,
                src.CUSTOMER_LAST_NAME,
                src.CUSTOMER_GENDER,
                src.CUSTOMER_DOB,
                src.CUSTOMER_JOB_ID,
                src.CUSTOMER_JOB_TITLE,
                src.CUSTOMER_JOB_CATEGORY_ID,
                src.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                src.CUSTOMER_WEALTH_SEGMENT);
         log_writer('MSG', obj_name, sysdate, null, 'Finish merge ' || SQL%ROWCOUNT || ' rows');
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS
            THEN  log_writer('MSG', obj_name, sysdate, null, 'Error: ' || SQLERRM);
            RAISE;
        
    END ld_sa_customer_online;    
-------------------------------------------------------------------------------------------------







END pkg_etl_sa_load;