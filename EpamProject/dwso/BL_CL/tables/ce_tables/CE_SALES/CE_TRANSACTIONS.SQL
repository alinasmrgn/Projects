INSERT INTO ce_sales (transaction_id,     
            transaction_src_id,
            transaction_source_system,
            transaction_source_table,
            transaction_date,
            CUSTOMER_ID,
            DELIVERY_ADDRESS_ID,
            EMPLOYEE_ID,
            PRODUCT_ID,
            CHANNEL_ID,
            PAYMENT_TYPE_ID,
            REVENUE,
            UNITY_PRICE,
            UNITY_COST,
            QUANTITY,                            
            TOTAL_PRICE,
            insert_dt,
            update_dt) 
SELECT  CE_TRANSACTIONS_S.nextval,
            scr_id,
            scr_table,
            scr_system,
            transaction_date,
            customer_id,
            delivery_addresses_id,
            employee_id, 
            product_id,
            channel_id,
            payment_type_id,
            (UNIT_PRICE - UNIT_COST) * QUANTITY,
            unit_price,
            unit_cost,
           quantity,
            UNIT_PRICE * QUANTITY,                            
            sysdate,
            sysdate
FROM    (
            SELECT   transaction_id as scr_id,
                    'SA_SALES_INSTORE' AS scr_table,
                    'SA_INSTORE' AS scr_system,
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS transaction_date,
                    COALESCE(cust.customer_id, -99) as customer_id,
                    COALESCE(addr_3nf.DELIVERY_ADDRESS_ID, -99) as delivery_addresses_id,
                    COALESCE(emp.employee_id, -99) as employee_id, 
                    COALESCE(p.product_id, -99) as product_id,
                    COALESCE(ch.channel_id, -99) as channel_id,  
                    COALESCE(st.payment_type_id, -99) as payment_type_id,               
                    REVENUE,   
                    UNIT_PRICE,
                    UNIT_COST,
                    quantity
            FROM sa_instore.sa_sales_instore2 ca
            LEFT OUTER JOIN ce_products p            ON p.product_src_id = to_char(ca.PRODUCT_SURR_ID)     AND p.product_source_system = 'SA_INSTORE'      
           
            LEFT OUTER JOIN (  Select ce.customer_id, mp.customer_scr_id, mp.customer_scr_system
                            From ce_customers ce 
                            Inner join bl_cl.map_ce_customers mp
                            On ce.customer_src_id = to_char(mp.customer_id) 
                                        ) cust        ON cust.customer_scr_id = to_char(ca.CUSTOMER_SURR_ID)     AND cust.customer_scr_system = 'SA_INSTORE'       
           
            LEFT OUTER JOIN ce_payment_types st         ON st.payment_type_src_id = to_char(ca.PAYMENT_TYPE_SURR_ID)  AND st.payment_type_source_system = 'SA_INSTORE'        
            LEFT OUTER JOIN ce_channels ch           ON ch.channel_src_id = to_char(ca.channel_surr_id)    AND ch.channel_source_system = 'SA_INSTORE'          
            LEFT OUTER JOIN ce_employees_scd emp     ON emp.EMPLOYEE_src_ID = to_char(ca.EMPLOYEE_SURR_ID)       AND emp.EMPLOYEE_SOURCE_system = 'SA_INSTORE'         
            LEFT OUTER JOIN ce_delivery_addresses addr_3nf            ON addr_3nf.DELIVERY_ADDRESS_src_ID = to_char(ca.DELIVERY_ADDRESS_SURR_ID)    AND addr_3nf.DELIVERY_ADDRESS_SOURCE_system = 'SA_INSTORE'      
            UNION ALL
            SELECT    transaction_id as scr_id,
                    'SA_SALES_ONLINE' AS scr_table,
                    'SA_ONLINE' AS scr_system,
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS transaction_date,
                   COALESCE(cust.customer_id, -99) as customer_id,
                    COALESCE(addr_3nf.DELIVERY_ADDRESS_ID, -99) as delivery_addresses_id,
                    COALESCE(emp.employee_id, -99) as employee_id, 
                    COALESCE(p.product_id, -99) as product_id,
                    COALESCE(ch.channel_id, -99) as channel_id,  
                    COALESCE(st.payment_type_id, -99) as payment_type_id,            
                    REVENUE, 
                    UNIT_PRICE,
                    UNIT_COST,
                     quantity
            FROM sa_online.sa_sales_online2 ca
      LEFT OUTER JOIN ce_products p            ON p.product_src_id = to_char(ca.PRODUCT_SURR_ID)     AND p.product_source_system = 'SA_INSTORE'      
           
            LEFT OUTER JOIN (  Select ce.customer_id, mp.customer_scr_id, mp.customer_scr_system
                            From ce_customers ce 
                            Inner join bl_cl.map_ce_customers mp
                            On ce.customer_src_id = to_char(mp.customer_id) 
                                        ) cust        ON cust.customer_scr_id = to_char(ca.CUSTOMER_SURR_ID)     AND cust.customer_scr_system = 'SA_ONLINE'       
           
            LEFT OUTER JOIN ce_payment_types st         ON st.payment_type_src_id = to_char(ca.PAYMENT_TYPE_SURR_ID)  AND st.payment_type_source_system = 'SA_INSTORE'        
            LEFT OUTER JOIN ce_channels ch           ON ch.channel_src_id = to_char(ca.channel_surr_id)    AND ch.channel_source_system = 'SA_INSTORE'          
            LEFT OUTER JOIN ce_employees_scd emp     ON emp.EMPLOYEE_src_ID = to_char(ca.EMPLOYEE_SURR_ID)       AND emp.EMPLOYEE_SOURCE_system = 'SA_INSTORE'         
            LEFT OUTER JOIN ce_delivery_addresses addr_3nf            ON addr_3nf.DELIVERY_ADDRESS_src_ID = to_char(ca.DELIVERY_ADDRESS_SURR_ID)    AND addr_3nf.DELIVERY_ADDRESS_SOURCE_system = 'SA_INSTORE'      
            	);
		
COMMIT;