--inserting into CE_CHANNELS_CLASSES
INSERT INTO CE_CHANNELS_CLASSES (CHANNELS_CLASS_ID,CHANNELS_CLASS_SRC_ID,CHANNELS_CLASS_SOURCE_SYSTEM,CHANNELS_CLASS_SOURCE_TABLE,CHANNELS_CLASS)
SELECT  CE_CHANNELS_CLASSES_S.nextval         AS CHANNELS_CLASS_ID, --using sequence
        CHANNEL_CLASS_ID                    AS CHANNELS_CLASS_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        CHANNEL_CLASS                  AS CHANNELS_CLASS
      
FROM    (
            SELECT DISTINCT CHANNEL_CLASS_ID, CHANNEL_CLASS      
            FROM SA_INSTORE.SA_CHANNELS
            order by CHANNEL_CLASS_ID
        ) ;
COMMIT;    
--------------------------------------------------------------------
--inserting into CE_CHANNELS
INSERT INTO CE_CHANNELS (CHANNEL_ID,CHANNEL_SRC_ID,CHANNEL_SOURCE_SYSTEM,CHANNEL_SOURCE_TABLE,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID )
SELECT  CE_CHANNELS_S.nextval         AS CHANNEL_ID, --using sequence
        CHANNEL_SURR_ID                    AS CHANNEL_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        CHANNEL_NAME                  AS CHANNEL_DESC,
        CHANNEL_CLASS                   as CHANNEL_CLASS,
        CHANNEL_CLASS_ID                as CHANNEL_CLASS_ID
      
FROM    (
            SELECT DISTINCT CHANNEL_SURR_ID, CHANNEL_NAME, CHANNEL_CLASS, CHANNEL_CLASS_ID     
            FROM SA_INSTORE.SA_CHANNELS
            order by CHANNEL_SURR_ID
        ) ;
COMMIT;    
--------------------------------------------------------------------
--inserting into CE_PAYMENT_TYPES
INSERT INTO CE_PAYMENT_TYPES (PAYMENT_TYPE_ID,PAYMENT_TYPE_SRC_ID,PAYMENT_TYPE_SOURCE_SYSTEM,PAYMENT_TYPE_SOURCE_TABLE,PAYMENT_TYPE_NAME)
SELECT  CE_PAYMENT_TYPES_S.nextval         AS PAYMENT_TYPE_ID,
        PAYMENT_TYPE_SURR_ID                    AS PAYMENT_TYPE_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        PAYMENT_TYPE_NAME                  AS PAYMENT_TYPE_NAME
    
FROM    (
            SELECT DISTINCT PAYMENT_TYPE_SURR_ID, PAYMENT_TYPE_NAME  
            FROM SA_INSTORE.SA_PAYMENT_TYPES
            order by PAYMENT_TYPE_SURR_ID
        ) ;
COMMIT;   
select * from CE_PAYMENT_TYPES;
--------------------------------------------------------------------
--inserting into CE_CUSTOMER_JOB_CATEGORIES
INSERT INTO CE_CUSTOMER_JOB_CATEGORIES (CUSTOMER_JOB_CATEGORY_ID,CUSTOMER_JOB_CATEGORY_SRC_ID,CUSTOMER_JOB_CATEGORY_SOURCE_SYSTEM,CUSTOMER_JOB_CATEGORY_SOURCE_TABLE,CUSTOMER_JOB_CATEGORY )
SELECT  CE_CUSTOMER_JOB_CATEGORIES_S.nextval         AS CUSTOMER_JOB_CATEGORY_ID, --using sequence
        CUSTOMER_JOB_CATEGORY_ID                    AS CUSTOMER_JOB_CATEGORY_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        CUSTOMER_JOB_INDUSTRY_CATEGORY                   as CUSTOMER_JOB_CATEGORY
      
FROM    (
            SELECT DISTINCT CUSTOMER_JOB_INDUSTRY_CATEGORY     
            FROM SA_INSTORE.SA_CUSTOMER_INSTORE
        ) ;
COMMIT;    
--------------------------------------------------------------------
--inserting into CE_CUSTOMER_JOBS
INSERT INTO CE_CUSTOMER_JOBS (CUSTOMER_JOB_ID,CUSTOMER_JOB_SRC_ID,CUSTOMER_JOB_SOURCE_SYSTEM,CUSTOMER_JOB_SOURCE_TABLE,CUSTOMER_JOB_TITLE, CUSTOMER_JOB_CATEGORY_ID)
SELECT  CE_CUSTOMER_JOBS_S.nextval         AS CUSTOMER_JOB_ID, --using sequence
        CUSTOMER_JOB_ID                    AS CUSTOMER_JOB_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        CUSTOMER_JOB_TITLE                   as CUSTOMER_JOB_TITLE,
        CUSTOMER_JOB_CATEGORY_ID        AS CUSTOMER_JOB_CATEGORY_ID
      
FROM    (
            SELECT DISTINCT CUSTOMER_JOB_INDUSTRY_CATEGORY     
            FROM SA_INSTORE.SA_CUSTOMER_INSTORE
        ) ;
COMMIT;    
--------------------------------------------------------------------
--inserting into CE_DELIVERY_STATES
INSERT INTO CE_DELIVERY_STATES (DELIVERY_STATE_ID,DELIVERY_STATE_SRC_ID,DELIVERY_STATE_SOURCE_SYSTEM,DELIVERY_STATE_SOURCE_TABLE, DELIVERY_STATE)
SELECT  CE_DELIVERY_STATES_S.nextval         AS DELIVERY_STATE_ID, --using sequence
        DELIVERY_STATE_ID                    AS DELIVERY_STATE_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        DELIVERY_STATE        AS DELIVERY_STATE
      
FROM    (
            SELECT DISTINCT DELIVERY_STATE_ID, DELIVERY_STATE     
            FROM SA_INSTORE.SA_DELIVERY_ADDRESS
        ) ;
COMMIT;    
select * from CE_DELIVERY_STATES;
--------------------------------------------------------------------
--inserting into CE_DELIVERY_ADDRESSES
INSERT INTO CE_DELIVERY_ADDRESSES (DELIVERY_ADDRESS_ID,DELIVERY_ADDRESS_SRC_ID,DELIVERY_ADDRESS_SOURCE_SYSTEM,DELIVERY_ADDRESS_SOURCE_TABLE, DELIVERY_STREET_ADDRESS,DELIVERY_POSTAL_CODE,DELIVERY_STATE_ID, DELIVERY_COUNTRY)
SELECT  CE_DELIVERY_ADDRESSES_S.nextval         AS DELIVERY_ADDRESS_ID, --using sequence
        DELIVERY_ADDRESS_SURR_ID                    AS DELIVERY_ADDRESS_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        DELIVERY_STREET_ADDRESS        AS DELIVERY_STREET_ADDRESS,
        DELIVERY_POSTAL_CODE        AS DELIVERY_POSTAL_CODE,
        DELIVERY_STATE_ID        AS DELIVERY_STATE_ID,
        DELIVERY_COUNTRY        AS DELIVERY_COUNTRY
      
FROM    (
            SELECT DISTINCT DELIVERY_ADDRESS_SURR_ID, DELIVERY_STREET_ADDRESS, DELIVERY_POSTAL_CODE,  DELIVERY_STATE_ID, DELIVERY_COUNTRY    
            FROM SA_INSTORE.SA_DELIVERY_ADDRESS
        ) ;
COMMIT;    
select * from CE_DELIVERY_ADDRESSES;
--------------------------------------------------------------------
INSERT INTO CE_EMPLOYEES (EMPLOYEE_ID,EMPLOYEE_SRC_ID,EMPLOYEE_SOURCE_SYSTEM,EMPLOYEE_SOURCE_TABLE,EMPLOYEE_FIRST_NAME,EMPLOYEE_LAST_NAME, EMPLOYEE_EMAIL, EMPLOYEE_PHONE, IS_ACTIVE)
SELECT  CE_EMPLOYEES_S.nextval         AS EMPLOYEE_ID,
        EMPLOYEE_SURR_ID                    AS EMPLOYEE_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        EMPLOYEE_FIRST_NAME                  AS EMPLOYEE_FIRST_NAME,
        EMPLOYEE_LAST_NAME                  AS EMPLOYEE_LAST_NAME,
        EMPLOYEE_EMAIL                  AS EMPLOYEE_EMAIL,
        EMPLOYEE_PHONE                  AS EMPLOYEE_PHONE,
        1                  AS IS_ACTIVE
    
FROM    (
            SELECT DISTINCT EMPLOYEE_SURR_ID, EMPLOYEE_FIRST_NAME, EMPLOYEE_LAST_NAME, EMPLOYEE_EMAIL, EMPLOYEE_PHONE   
            FROM SA_INSTORE.SA_EMPLOYEES
            order by EMPLOYEE_SURR_ID
        ) ;
COMMIT;   
SELECT * FROM CE_EMPLOYEES;
--------------------------------------------------------------------
--inserting into CE_PRODUCT_BRANDS
INSERT INTO CE_PRODUCT_BRANDS (PRODUCT_BRAND_ID,PRODUCT_BRAND_SRC_ID,PRODUCT_BRAND_SOURCE_SYSTEM,PRODUCT_BRAND_SOURCE_TABLE,PRODUCT_BRAND)
SELECT  CE_PRODUCT_BRANDS_S.nextval         AS PRODUCT_BRAND_ID, --using sequence
        CE_PRODUCT_BRANDS_S.nextval                    AS PRODUCT_BRAND_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        PRODUCT_BRAND        AS PRODUCT_BRAND
      
FROM    (
            SELECT DISTINCT PRODUCT_BRAND    
            FROM SA_INSTORE.SA_PRODUCTS
        ) ;
COMMIT;    
select * from CE_PRODUCT_BRANDS;
--------------------------------------------------------------------
--inserting into CE_PRODUCTS
INSERT INTO CE_PRODUCTS (PRODUCT_ID,PRODUCT_SRC_ID,PRODUCT_SOURCE_SYSTEM,PRODUCT_SOURCE_TABLE,PRODUCT_BRAND_ID,PRODUCT_LINE,PRODUCT_SIZE,PRODUCT_CLASS )
SELECT  CE_PRODUCTS_S.nextval         AS PRODUCT_ID, --using sequence
        PRODUCT_SURR_ID                  AS PRODUCT_SRC_ID,
        'SA_BICYCLE'                 AS source_system,
        'SA_INSTORE'                 AS source_table,
        PRODUCT_BRAND_SRC_ID        AS PRODUCT_BRAND_ID,
        PRODUCT_LINE        AS PRODUCT_LINE,
        PRODUCT_SIZE        AS PRODUCT_SIZE,
        PRODUCT_CLASS        AS PRODUCT_CLASS
      
FROM    (
            SELECT DISTINCT PRODUCT_SURR_ID, PRODUCT_BRAND_SRC_ID, PRODUCT_LINE, PRODUCT_SIZE, PRODUCT_CLASS  
            FROM SA_INSTORE.SA_PRODUCTS SISP
            INNER JOIN CE_PRODUCT_BRANDS CPB ON CPB.PRODUCT_BRAND = SISP.PRODUCT_BRAND
        ) ;
COMMIT;    
select * from CE_PRODUCTS;
--------------------------------------------------------------------
--inserting into CE_TRANSACTIONS
INSERT INTO CE_TRANSACTIONS (TRANSACTION_ID,TRANSACTION_SRC_ID,TRANSACTION_SOURCE_SYSTEM,TRANSACTION_SOURCE_TABLE,CUSTOMER_ID, DELIVERY_ADDRESS_ID,EMPLOYEE_ID, PRODUCT_ID ,CHANNEL_ID,PAYMENT_TYPE_ID )
WITH ALL_SALES AS
(SELECT * FROM
    ( select 
            'SA_SALES_INSTORE'                 AS source_system,
            'SA_INSTORE'                 AS source_table,
            CUSTOMER_SURR_ID,
            DELIVERY_ADDRESS_SURR_ID,
            EMPLOYEE_SURR_ID,
            PRODUCT_SURR_ID ,
            CHANNEL_SURR_ID,
            PAYMENT_TYPE_SURR_ID
    FROM SA_INSTORE.SA_SALES_INSTORE)

UNION ALL 
SELECT * FROM
    ( select 
            'SA_SALES_ONLINE'                 AS source_system,
            'SA_ONLINE'                 AS source_table,
            CUSTOMER_SURR_ID,
            DELIVERY_ADDRESS_SURR_ID,
            EMPLOYEE_SURR_ID,
            PRODUCT_SURR_ID ,
            CHANNEL_SURR_ID,
            PAYMENT_TYPE_SURR_ID
    FROM SA_ONLINE.SA_SALES_ONLINE)
)   

SELECT CE_TRANSACTIONS_S.NEXTVAL,
        CE_TRANSACTIONS_S.NEXTVAL,
        source_system,
        source_table,
         COALESCE(CUST.CUSTOMER_ID, -99),
        COALESCE(DEL_AD.DELIVERY_ADDRESS_ID, -99),
        COALESCE(EMP.EMPLOYEE_ID, -99),
        COALESCE(PROD.PRODUCT_ID, -99),
        COALESCE(CHANN.CHANNEL_ID, -99),
        COALESCE(PT.PAYMENT_TYPE_ID, -99)
FROM ALL_SALES
LEFT JOIN CE_CUSTOMERS CUST ON CUST.CUSTOMER_SRC_ID = ALL_SALES.CUSTOMER_SURR_ID
                                        AND CUST.CUSTOMER_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_DELIVERY_ADDRESSES DEL_AD ON DEL_AD.DELIVERY_ADDRESS_SRC_ID = ALL_SALES.DELIVERY_ADDRESS_SURR_ID
                                        AND DEL_AD.DELIVERY_ADDRESS_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_EMPLOYEES EMP ON EMP.EMPLOYEE_SRC_ID = ALL_SALES.EMPLOYEE_SURR_ID
                                        AND EMP.EMPLOYEE_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_PRODUCTS PROD ON PROD.PRODUCT_SRC_ID = ALL_SALES.PRODUCT_SURR_ID
                                        AND PROD.PRODUCT_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_CHANNELS CHANN ON CHANN.CHANNEL_SRC_ID = ALL_SALES.CHANNEL_SURR_ID
                                        AND CHANN.CHANNEL_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_PAYMENT_TYPES PT ON PT.PAYMENT_TYPE_SRC_ID = ALL_SALES.PAYMENT_TYPE_SURR_ID
                                        AND PT.PAYMENT_TYPE_SOURCE_SYSTEM = ALL_SALES.source_system;
        
    
 
select * from CE_TRANSACTIONS;
--------------------------------------------------------------------
--inserting into CE_PRODUCTS_TRANSACTIONS
INSERT INTO CE_PRODUCTS_TRANSACTIONS (PRODUCT_ID,TRANSACTION_ID,UNIT_PRICE,UNIT_COST)
WITH ALL_SALES AS
( select 
            'SA_SALES_INSTORE'                 AS source_system,
            'SA_INSTORE'                 AS source_table,
            CUSTOMER_SURR_ID,
            DELIVERY_ADDRESS_SURR_ID,
            EMPLOYEE_SURR_ID,
            PRODUCT_SURR_ID ,
            CHANNEL_SURR_ID,
            PAYMENT_TYPE_SURR_ID,
            UNIT_PRICE,
            UNIT_COST
    FROM SA_INSTORE.SA_SALES_INSTORE

UNION ALL 

    select 
            'SA_SALES_ONLINE'                 AS source_system,
            'SA_ONLINE'                 AS source_table,
            CUSTOMER_SURR_ID,
            DELIVERY_ADDRESS_SURR_ID,
            EMPLOYEE_SURR_ID,
            PRODUCT_SURR_ID ,
            CHANNEL_SURR_ID,
            PAYMENT_TYPE_SURR_ID,
            UNIT_PRICE,
            UNIT_COST
    FROM SA_ONLINE.SA_SALES_ONLINE),

SALES AS 
(SELECT
        source_system,
        source_table,
         COALESCE(CUST.CUSTOMER_ID, -99),
        COALESCE(DEL_AD.DELIVERY_ADDRESS_ID, -99),
        COALESCE(EMP.EMPLOYEE_ID, -99),
        COALESCE(PROD.PRODUCT_ID, -99),
        COALESCE(CHANN.CHANNEL_ID, -99),
        COALESCE(PT.PAYMENT_TYPE_ID, -99),
        UNIT_PRICE,
        UNIT_COST
FROM ALL_SALES
LEFT JOIN CE_CUSTOMERS CUST ON CUST.CUSTOMER_SRC_ID = ALL_SALES.CUSTOMER_SURR_ID
                                        AND CUST.CUSTOMER_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_DELIVERY_ADDRESSES DEL_AD ON DEL_AD.DELIVERY_ADDRESS_SRC_ID = ALL_SALES.DELIVERY_ADDRESS_SURR_ID
                                        AND DEL_AD.DELIVERY_ADDRESS_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_EMPLOYEES EMP ON EMP.EMPLOYEE_SRC_ID = ALL_SALES.EMPLOYEE_SURR_ID
                                        AND EMP.EMPLOYEE_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_PRODUCTS PROD ON PROD.PRODUCT_SRC_ID = ALL_SALES.PRODUCT_SURR_ID
                                        AND PROD.PRODUCT_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_CHANNELS CHANN ON CHANN.CHANNEL_SRC_ID = ALL_SALES.CHANNEL_SURR_ID
                                        AND CHANN.CHANNEL_SOURCE_SYSTEM = ALL_SALES.source_system
LEFT JOIN CE_PAYMENT_TYPES PT ON PT.PAYMENT_TYPE_SRC_ID = ALL_SALES.PAYMENT_TYPE_SURR_ID
                                        AND PT.PAYMENT_TYPE_SOURCE_SYSTEM = ALL_SALES.source_system)

SELECT 
        CE_TRANSACTIONS_S.NEXTVAL AS TRANS_ID,
        COALESCE(PROD.PRODUCT_ID, -99),
        SALES.UNIT_PRICE,
        SALES.UNIT_COST
FROM SALES
LEFT JOIN CE_TRANSACTIONS TR ON TR.TRANSACTION_SOURCE_SYSTEM = SALES.source_system
                            AND TR.TRANSACTION_SOURCE_TABLE = SALES.source_table
                             AND TR.PRODUCT_ID = SALES.PRODUCT_SURR_ID
LEFT JOIN CE_PRODUCTS PROD ON PROD.PRODUCT_SRC_ID = SALES.PRODUCT_SURR_ID
                            AND PROD.PRODUCT_SOURCE_SYSTEM = SALES.source_system;
        
--------------------------------------------------------------------
--inserting into CE_TRANSACTIONS
INSERT INTO CE_CUSTOMERS (CUSTOMER_ID,CUSTOMER_SRC_ID,CUSTOMER_SOURCE_SYSTEM,CUSTOMER_SOURCE_TABLE,CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME,CUSTOMER_GENDER, CUSTOMER_DOB , CUSTOMER_WEALTH_SEGMENT,CUSTOMER_JOB_ID  )
WITH ALL_CUSTOMERS AS
    ( select CUSTOMER_SURR_ID,
            'SA_INSTORE'                AS source_system,
            'SA_CUSTOMER_INSTORE'                 AS source_table,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            TO_DATE(CUSTOMER_DOB, 'YYYY-MM-DD' ) AS CUSTOMER_DOB,
            CUSTOMER_WEALTH_SEGMENT,
            CUSTOMER_JOB_ID
    FROM SA_INSTORE.SA_CUSTOMER_INSTORE)

UNION ALL 
    ( select CUSTOMER_SURR_ID,
            'SA_ONLINE'                AS source_system,
            'SA_CUSTOMER_ONLINE'                 AS source_table,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            TO_DATE(CUSTOMER_DOB, 'YYYY-MM-DD' ) AS CUSTOMER_DOB,
            CUSTOMER_WEALTH_SEGMENT,
            CUSTOMER_JOB_ID
    FROM SA_ONLINE.SA_CUSTOMER_ONLINE)

SELECT CE_CUSTOMERS_S.NEXTVAL,
        CUSTOMER_SURR_ID
        source_system,
        source_table,
        USTOMER_FIRST_NAME,
        CUSTOMER_LAST_NAME,
        CUSTOMER_GENDER,
        CUSTOMER_DOB,
        CUSTOMER_WEALTH_SEGMENT,
        CUSTOMER_JOB_ID
FROM ALL_CUSTOMERS;

select * from CE_CUSTOMERS;
--------------------------------------------------------------------
