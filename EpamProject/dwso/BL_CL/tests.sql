--ce_sales
SELECT 
    TRANSACTION_ID,
    TRANSACTION_DATE,
    CUSTOMER_ID,
    DELIVERY_ADDRESS_ID,
    EMPLOYEE_ID,
    PRODUCT_ID,
    CHANNEL_ID,
    PAYMENT_TYPE_ID,
    count(*)
FROM ce_sales
GROUP BY TRANSACTION_ID,
    TRANSACTION_DATE,
    CUSTOMER_ID,
    DELIVERY_ADDRESS_ID,
    EMPLOYEE_ID,
    PRODUCT_ID,
    CHANNEL_ID,
    PAYMENT_TYPE_ID
HAVING count(*) > 1;
-------------------------------------------------------------
WITH all_load_test AS 
    (SELECT count(*) as err_cnt FROM
        (( SELECT   transaction_id  , 
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS TRANSACTION_DATE,
                    NVL(c.customer_id, -99) as customer_id,
                    NVL(da.DELIVERY_ADDRESS_ID, -99) as delivery_addresses_id,
                    NVL(e.employee_id, -99) as employee_id,  
                    NVL(p.product_id, -99) as product_id,
                    NVL(ch.channel_id, -99) as channel_id,  
                    NVL(pt.payment_type_id, -99) as payment_type_id
            FROM sa_instore.sa_sales_instore3 s
            LEFT OUTER JOIN  (   
                            Select ce.customer_id, mp.customer_scr_id, mp.customer_scr_system,  mp.customer_scr_table
                            From ce_customers ce 
                            Inner join bl_cl.map_ce_customers mp
                            On ce.customer_src_id = to_char(mp.customer_id)             
                        ) c                                                         
                                                        ON TO_CHAR(s.CUSTOMER_SURR_ID) = c.customer_scr_id                                              
                                                       
            LEFT OUTER JOIN ce_delivery_addresses da        ON TO_CHAR(s.DELIVERY_ADDRESS_SURR_ID) = da.DELIVERY_ADDRESS_src_ID                                           
                                                         AND da.DELIVERY_ADDRESS_SOURCE_TABLE = 'SA_INSTORE'
                                                          AND da.DELIVERY_ADDRESS_SOURCE_SYSTEM = 'SA_BICYCLE'
                                                          
            LEFT OUTER JOIN   ce_employees_scd e   ON  TO_CHAR(s.EMPLOYEE_SURR_ID) = e.EMPLOYEE_src_ID                                            
                                                        AND e.EMPLOYEE_SOURCE_table = 'SA_EMPLOYEES'                                        
                                                       AND e.EMPLOYEE_SOURCE_SYSTEM = 'SA_INSTORE'
                                                       
            LEFT OUTER JOIN  ce_products p       ON TO_CHAR(s.PRODUCT_SURR_ID) = p.product_src_id 
                                                        AND p.product_source_TABLE = 'SA_INSTORE'  
                                                        AND p.product_source_system = 'SA_BICYCLE' 
                                                        
            LEFT OUTER JOIN ce_channels ch       ON TO_CHAR(s.CHANNEL_SURR_ID) = ch.channel_src_id                                           
                                                       AND ch.channel_source_TABLE = 'SA_INSTORE'
                                                       AND ch.channel_source_system = 'SA_BICYCLE'
                                                       
            LEFT OUTER JOIN ce_payment_types pt   ON TO_CHAR(s.PAYMENT_TYPE_SURR_ID) = pt.payment_type_src_id                                           
                                                      AND pt.payment_type_source_TABLE = 'SA_INSTORE'
                                                      AND pt.payment_type_source_system = 'SA_BICYCLE'
            
            UNION ALL
            SELECT  transaction_id  , 
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS TRANSACTION_DATE,
                    NVL(c.customer_id, -99) as customer_id,
                    NVL(da.DELIVERY_ADDRESS_ID, -99) as delivery_addresses_id,
                    NVL(e.employee_id, -99) as employee_id,  
                    NVL(p.product_id, -99) as product_id,
                    NVL(ch.channel_id, -99) as channel_id,  
                    NVL(pt.payment_type_id, -99) as payment_type_id
            FROM sa_online.sa_sales_online3 s
      LEFT OUTER JOIN  (   
                            Select ce.customer_id, mp.customer_scr_id, mp.customer_scr_system,  mp.customer_scr_table
                            From ce_customers ce 
                            Inner join bl_cl.map_ce_customers mp
                            On ce.customer_src_id = to_char(mp.customer_id)             
                        ) c                                                         
                                                        ON TO_CHAR(s.CUSTOMER_SURR_ID) = c.customer_scr_id                                              
                                                       
            LEFT OUTER JOIN ce_delivery_addresses da        ON TO_CHAR(s.DELIVERY_ADDRESS_SURR_ID) = da.DELIVERY_ADDRESS_src_ID                                           
                                                         AND da.DELIVERY_ADDRESS_SOURCE_TABLE = 'SA_INSTORE'
                                                          AND da.DELIVERY_ADDRESS_SOURCE_SYSTEM = 'SA_BICYCLE'
                                                          
            LEFT OUTER JOIN   ce_employees_scd e   ON  TO_CHAR(s.EMPLOYEE_SURR_ID) = e.EMPLOYEE_src_ID                                            
                                                        AND e.EMPLOYEE_SOURCE_table = 'SA_EMPLOYEES'                                        
                                                       AND e.EMPLOYEE_SOURCE_SYSTEM = 'SA_INSTORE'
                                                       
            LEFT OUTER JOIN  ce_products p       ON TO_CHAR(s.PRODUCT_SURR_ID) = p.product_src_id 
                                                        AND p.product_source_TABLE = 'SA_INSTORE'  
                                                        AND p.product_source_system = 'SA_BICYCLE' 
                                                        
            LEFT OUTER JOIN ce_channels ch       ON TO_CHAR(s.CHANNEL_SURR_ID) = ch.channel_src_id                                           
                                                       AND ch.channel_source_TABLE = 'SA_INSTORE'
                                                       AND ch.channel_source_system = 'SA_BICYCLE'
                                                       
            LEFT OUTER JOIN ce_payment_types pt   ON TO_CHAR(s.PAYMENT_TYPE_SURR_ID) = pt.payment_type_src_id                                           
                                                      AND pt.payment_type_source_TABLE = 'SA_INSTORE'
                                                      AND pt.payment_type_source_system = 'SA_BICYCLE')
        
        MINUS
        
        SELECT 
           TRANSACTION_ID,
            TRANSACTION_DATE,
            CUSTOMER_ID,
            DELIVERY_ADDRESS_ID,
            EMPLOYEE_ID,
            PRODUCT_ID,
            CHANNEL_ID,
            PAYMENT_TYPE_ID
        FROM ce_sales))
SELECT 
    CASE 
        WHEN err_cnt = 0 THEN 'All rows from source exist in ce_sales'
        ELSE 'Not all rows from source exist in ce_sales'
    END test_result
FROM all_load_test;

--fct_sales
SELECT 
    customer_surr_id,
    DELIVERY_ADDRESS_SURR_ID,
    EMPLOYEE_SURR_ID,
    PRODUCT_SURR_ID,
    CHANNEL_SURR_ID,
    PAYMENT_TYPE_SURR_ID,
    date_id,
    count(*)
FROM fct_sales
GROUP BY customer_surr_id,
    DELIVERY_ADDRESS_SURR_ID,
    EMPLOYEE_SURR_ID,
    PRODUCT_SURR_ID,
    CHANNEL_SURR_ID,
    PAYMENT_TYPE_SURR_ID,
    date_id
HAVING count(*) > 1;
    
WITH  all_load_test AS 
    (SELECT COUNT(*) as err_cnt FROM
        (SELECT 
            nvl(dim_c.customer_surr_id, -99) as customer_surr_id,
                        nvl(dim_da.DELIVERY_ADDRESS_SURR_ID, -99) as DELIVERY_ADDRESS_SURR_ID,
                        nvl(dim_e.EMPLOYEE_SURR_ID, -99) as EMPLOYEE_SURR_ID,
                        nvl(dim_p.PRODUCT_SURR_ID, -99) as PRODUCT_SURR_ID,
                        nvl(dim_ch.CHANNEL_SURR_ID, -99) as CHANNEL_SURR_ID,
                        nvl(dim_pt.PAYMENT_TYPE_SURR_ID, -99) as PAYMENT_TYPE_SURR_ID,
                        s.transaction_date
        FROM ce_sales s
               
                Left join ce_channels ch on s.channel_id = ch.channel_id
                Left join dim_channels dim_ch on to_char(ch.channel_id) = dim_ch.source_id AND dim_ch.source_table = 'CE_CHANNELS' AND dim_ch.source_system = 'BL_3NF'
                Left join ce_customers c on s.customer_id = c.customer_id
                Left join dim_customers dim_c on to_char(c.customer_id) = dim_c.source_id AND dim_c.source_table = 'CE_CUSTOMERS'  AND dim_c.source_system = 'BL_3NF'
                Left join ce_delivery_addresses da on s.delivery_address_id = da.delivery_address_id
                Left join dim_delivery_addresses dim_da on to_char(da.delivery_address_id) = dim_da.source_id  AND dim_da.source_table = 'CE_DELIVERY_ADDRESSES' AND dim_da.source_system = 'BL_3NF'
               Left join ce_employees_scd e on s.employee_id = e.employee_id --AND s.transaction_date > = e.start_dt               AND s.transaction_date < e.end_dt
                Left join dim_employees_scd dim_e on to_char(e.employee_id) = dim_e.source_id AND dim_e.start_dt = e.start_dt  AND dim_e.source_table = 'CE_EMPLOYEES_SCD'        
                                                                                                                                 AND dim_e.source_system = 'BL_3NF'
              Left join ce_payment_types pt on s.payment_type_id = pt.payment_type_id
                Left join dim_payment_types dim_pt on to_char(pt.payment_type_id) = dim_pt.source_id AND dim_pt.source_table = 'CE_PAYMENT_TYPES'  AND dim_pt.source_system = 'BL_3NF'
                Left join ce_products p on s.product_id = p.product_id
                Left join dim_products dim_p on to_char(p.product_id) = dim_p.source_id AND dim_p.source_table = 'CE_PRODUCTS'  AND dim_p.source_system = 'BL_3NF'   
                

        MINUS        
        SELECT 
            customer_surr_id,
            DELIVERY_ADDRESS_SURR_ID,
            EMPLOYEE_SURR_ID,
            PRODUCT_SURR_ID,
            CHANNEL_SURR_ID,
            PAYMENT_TYPE_SURR_ID,
            date_id
        FROM fct_sales))
SELECT 
    CASE 
        WHEN err_cnt = 0 THEN 'All rows from ce_sales exist in fct_sales'
        ELSE 'Not all rowsfrom ce_sales exist in fct_sales'
    END test_result
FROM all_load_test;    
    
