
CREATE OR REPLACE PACKAGE BODY pkg_etl_ce_sales
AS
    PROCEDURE etl_ce_sales AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); 
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_sales');
     
       Merge into CE_SALES ce
    Using ( 
            WITH all_sales AS
            (SELECT   transaction_id,
                    'SA_SALES_INSTORE' AS src_table,
                    'SA_INSTORE' AS src_system,
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS transaction_date,
                    CUSTOMER_SURR_ID,
                    DELIVERY_ADDRESS_SURR_ID ,
                    EMPLOYEE_SURR_ID,
                    PRODUCT_SURR_ID,
                    CHANNEL_SURR_ID, 
                    PAYMENT_TYPE_SURR_ID,               
                    REVENUE,   
                    UNIT_PRICE,
                    UNIT_COST,
                    quantity
            FROM sa_instore.sa_sales_instore2 
            UNION ALL
            SELECT    transaction_id,
                    'SA_SALES_ONLINE' AS src_table,
                    'SA_ONLINE' AS src_system,
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS transaction_date,
                    CUSTOMER_SURR_ID,
                    DELIVERY_ADDRESS_SURR_ID ,
                    EMPLOYEE_SURR_ID,
                    PRODUCT_SURR_ID,
                    CHANNEL_SURR_ID, 
                    PAYMENT_TYPE_SURR_ID,               
                    REVENUE, 
                    UNIT_PRICE,
                    UNIT_COST,
                     quantity
            FROM sa_online.sa_sales_online2)
            
            SELECT  transaction_id,
                    src_table,
                    src_system,
                    transaction_date,
                    COALESCE(c.customer_id, -99) as customer_id,
                    COALESCE(da.DELIVERY_ADDRESS_ID, -99) as delivery_addresses_id,
                    COALESCE(e.employee_id, -99) as employee_id,     
                    COALESCE(ch.channel_id, -99) as channel_id,  
                    COALESCE(p.product_id, -99) as product_id,
                    COALESCE(pt.payment_type_id, -99) as payment_type_id,
                    REVENUE, 
                    UNIT_PRICE,
                    UNIT_COST,
                    quantity
            FROM all_sales
            LEFT OUTER JOIN  (   
                            Select ce.customer_id, mp.customer_scr_id, mp.customer_scr_system
                            From ce_customers ce 
                            Inner join bl_cl.map_ce_customers mp
                            On ce.customer_src_id = to_char(mp.customer_id)             
                        ) c                                                         
                                                        ON TO_CHAR(all_sales.CUSTOMER_SURR_ID) = c.customer_scr_id                                              
                                                        AND c.customer_scr_system = all_sales.src_system                           
            
            LEFT OUTER JOIN ce_delivery_addresses da        ON TO_CHAR(all_sales.DELIVERY_ADDRESS_SURR_ID) = da.DELIVERY_ADDRESS_src_ID                                           
                                                         AND all_sales.src_system = da.DELIVERY_ADDRESS_SOURCE_TABLE    
                                                          
            LEFT OUTER JOIN   ce_employees_scd e   ON  TO_CHAR(all_sales.EMPLOYEE_SURR_ID) = e.EMPLOYEE_src_ID                                            
                                                        AND e.EMPLOYEE_SOURCE_system = all_sales.src_system                                        
                                                       
                                                       
            LEFT OUTER JOIN  ce_products p       ON TO_CHAR(all_sales.PRODUCT_SURR_ID) = p.product_src_id 
                                                        AND p.product_source_TABLE = all_sales.src_system   
                                                        
            LEFT OUTER JOIN ce_channels ch       ON TO_CHAR(all_sales.CHANNEL_SURR_ID) = ch.channel_src_id                                           
                                                       AND ch.channel_source_TABLE = all_sales.src_system
                                                       
            LEFT OUTER JOIN ce_payment_types pt   ON TO_CHAR(all_sales.PAYMENT_TYPE_SURR_ID) = pt.payment_type_src_id                                           
                                                      AND pt.payment_type_source_TABLE = all_sales.src_system
                                                   
            
        ) sa
ON  (
        ce.transaction_src_id = to_char(sa.transaction_id)
        and ce.transaction_source_table = sa.src_table
        and ce.transaction_source_system = sa.src_system
    )
WHEN MATCHED THEN
    UPDATE SET  ce.DELIVERY_ADDRESS_ID = sa.delivery_addresses_id,
                ce.EMPLOYEE_ID = sa.employee_id,
                ce.CUSTOMER_ID = sa.customer_id,
                ce.PRODUCT_ID = sa.product_id,
                ce.PAYMENT_TYPE_ID = sa.payment_type_id,
                ce.CHANNEL_ID = sa.channel_id,
                ce.UNITY_PRICE = sa.unit_price,
                ce.UNITY_COST = sa.unit_cost,
                ce.update_dt = sysdate
    WHERE   decode(ce.DELIVERY_ADDRESS_ID, sa.delivery_addresses_id, 0, 1) + 
            decode(ce.employee_id, sa.employee_id, 0, 1) + 
            decode(ce.customer_id, sa.customer_id, 0, 1) + 
            decode(ce.product_id, sa.product_id, 0, 1) + 
            decode(ce.payment_type_id, sa.payment_type_id, 0, 1) + 
            decode(ce.channel_id, sa.channel_id, 0, 1) + 
            decode(ce.UNITY_COST, sa.unit_cost, 0, 1) + 
            decode(ce.UNITY_PRICE, sa.unit_price, 0, 1) > 0
WHEN NOT MATCHED THEN
    INSERT (transaction_id,     
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
    
    VALUES (CE_TRANSACTIONS_S.nextval,
            sa.transaction_id,
            sa.src_table,
            sa.src_system,
            sa.transaction_date,
            sa.customer_id,
            sa.delivery_addresses_id,
            sa.employee_id, 
            sa.product_id,
            sa.channel_id,
            sa.payment_type_id,
            (sa.UNIT_PRICE - sa.UNIT_COST) * sa.QUANTITY,
            sa.unit_price,
            sa.unit_cost,
           sa.quantity,
            sa.UNIT_PRICE * sa.QUANTITY,                            
            sysdate,
            sysdate);         
                     
                    
       
       
        log_writer('MSG', obj_name, sysdate, null, 'Merge into ce_sales ' || SQL%ROWCOUNT || ' rows.');
         COMMIT;
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_sales;
        
END pkg_etl_ce_sales;

exec pkg_etl_ce_sales.etl_ce_sales;
