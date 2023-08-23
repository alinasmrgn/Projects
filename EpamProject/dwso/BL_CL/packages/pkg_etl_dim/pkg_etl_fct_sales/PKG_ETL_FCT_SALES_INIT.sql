
CREATE OR REPLACE PACKAGE BODY pkg_etl_fct_sales AS
-----------------------------------------------------------------------------------------------------------------
 PROCEDURE ld_wrk_sales
    AS
    row_merged NUMERIC;
    obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
    
        log_writer('MSG', obj_name, sysdate, null, 'Insert data into table wrk_sales');

        INSERT INTO wrk_sales(
            DATE_ID,
            TRANSACTION_ID,
            CUSTOMER_SURR_ID,
            DELIVERY_ADDRESS_SURR_ID,
            EMPLOYEE_SURR_ID,
            PRODUCT_SURR_ID,
            CHANNEL_SURR_ID,
            PAYMENT_TYPE_SURR_ID,
            UNIT_PRICE,
            UNIT_COST,
            QUANTITY,
            SRCSYSTEM,
            SRCTABLE,
            UPDATE_DT)
        SELECT      TO_DATE(DATE_ID, 'DD-MM-YYYY') AS DATE_ID,
                    transaction_id,
                    CUSTOMER_SURR_ID,
                    DELIVERY_ADDRESS_SURR_ID ,
                    EMPLOYEE_SURR_ID,
                    PRODUCT_SURR_ID,
                    CHANNEL_SURR_ID, 
                    PAYMENT_TYPE_SURR_ID,                 
                    UNIT_PRICE,
                    UNIT_COST,
                    quantity,
                     'SA_INSTORE' AS SRCSYSTEM,
                    'SA_SALES_INSTORE' AS SRCTABLE,
                    sysdate as update_dt
            FROM sa_instore.sa_sales_instore3 
            WHERE update_dt > (SELECT PREVIOUS_LOAD_DATE FROM PRM_MTA_INCREMENTAL_LOAD  WHERE sa_table_name='SA_SALES_INSTORE')
            UNION ALL
            SELECT  
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS DATE_ID,  
                    transaction_id,
                    CUSTOMER_SURR_ID,
                    DELIVERY_ADDRESS_SURR_ID ,
                    EMPLOYEE_SURR_ID,
                    PRODUCT_SURR_ID,
                    CHANNEL_SURR_ID, 
                    PAYMENT_TYPE_SURR_ID,    
                    UNIT_PRICE,
                    UNIT_COST,
                     quantity,
                      'SA_ONLINE' AS SRCSYSTEM,
                    'SA_SALES_ONLINE' AS SRCTABLE,
                     sysdate as update_dt
            FROM sa_online.sa_sales_online3
            WHERE update_dt > (SELECT PREVIOUS_LOAD_DATE FROM PRM_MTA_INCREMENTAL_LOAD  WHERE sa_table_name='SA_SALES_ONLINE');
      
       row_merged := SQL%ROWCOUNT;    
       log_writer('MSQ', obj_name, sysdate, null, 'Finish insert ' || SQL%ROWCOUNT || ' rows.');
        COMMIT;
        
        IF row_merged > 0 THEN
            log_writer('MSQ', obj_name, sysdate, null, 'Updating PREVIOUS_LOAD_DATE in PRM_MTA_INCREMENTAL_LOAD');
            UPDATE PRM_MTA_INCREMENTAL_LOAD
                SET previous_load_date = SYSDATE
                WHERE sa_table_name = 'SA_SALES_INSTORE';
            UPDATE PRM_MTA_INCREMENTAL_LOAD
                SET previous_load_date = SYSDATE
                WHERE sa_table_name = 'SA_SALES_ONLINE';
            COMMIT;
        END IF;
        
        EXCEPTION
            WHEN OTHERS
            THEN log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
            RAISE;

    END ld_wrk_sales;
    -------------------------------------------------------------------------------------------------------------------
    PROCEDURE etl_ce_sales  
    as
         row_inserted NUMERIC;
         obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); 
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start insert into ce_sales');
     
           INSERT INTO ce_sales (
            transaction_id,     
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
        WITH sa AS (
            SELECT
                    transaction_id as transaction_src_id,
                    SRCSYSTEM,
                    SRCTABLE,
                    TO_DATE(DATE_ID, 'DD-MM-YYYY') AS DATE_ID,
                    NVL(c.customer_id, -99) as customer_id,
                    NVL(da.DELIVERY_ADDRESS_ID, -99) as delivery_addresses_id,
                    NVL(e.employee_id, -99) as employee_id,     
                    NVL(ch.channel_id, -99) as channel_id,  
                    NVL(p.product_id, -99) as product_id,
                    NVL(pt.payment_type_id, -99) as payment_type_id, 
                    UNIT_PRICE,
                    UNIT_COST,
                    quantity
           FROM wrk_sales s
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
    WHERE s.update_dt > (SELECT previous_load_date FROM PRM_MTA_INCREMENTAL_LOAD  WHERE sa_table_name='wrk_sales')
            GROUP BY DATE_ID,  --using group by here to avoid duplicates after merging sources and customers deduplication
                    NVL(c.customer_id, -99) ,
                    NVL(da.DELIVERY_ADDRESS_ID, -99) ,
                    NVL(e.employee_id, -99),     
                    NVL(ch.channel_id, -99) ,  
                    NVL(p.product_id, -99) ,
                    NVL(pt.payment_type_id, -99),transaction_id,SRCSYSTEM,SRCTABLE,UNIT_PRICE,UNIT_COST,quantity )     
        SELECT          
            CE_TRANSACTIONS_S.nextval,
            sa.transaction_src_id,
            sa.SRCSYSTEM,
            sa.SRCTABLE,
            sa.DATE_ID,
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
            sysdate
        FROM sa;
       
                                    
             row_inserted := SQL%ROWCOUNT;      
            log_writer('MSG', obj_name, sysdate, null, 'Finish insert' || SQL%ROWCOUNT || ' rows.');
            COMMIT;


        IF row_inserted > 0 THEN
            log_writer('MSG', obj_name, sysdate, null, 'Updating PREVIOUS_LOAD_DATE in PRM_MTA_INCREMENTAL_LOAD');
            UPDATE PRM_MTA_INCREMENTAL_LOAD
                SET previous_load_date = SYSDATE
                WHERE sa_table_name = 'wrk_sales';
        END IF;


        EXCEPTION
         WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
            RAISE;
    END etl_ce_sales;

------------------------------------------------------------------------------------------------------------
--  partition exhange with  

    PROCEDURE etl_fct_sales(start_dt varchar2,end_dt varchar2) 
    IS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
       cursor fct_sales_curs is 
    select greatest(to_date(start_dt,'dd-mm-yy'), trunc(add_months( to_date(start_dt,'dd-mm-yy'), 3 * (level - 1)), 'Q')) as startquarter,
           least(to_date(end_dt,'dd-mm-yy'), last_day(add_months( to_date(start_dt,'dd-mm-yy'), 3 * (level - 1) + 2))) as endquarter,
           TO_CHAR(greatest(to_date(start_dt,'dd-mm-yy'), trunc(add_months( to_date(start_dt,'dd-mm-yy'), 3 * (level - 1)), 'Q')), 'Q') as quarter,
           TO_CHAR(greatest(to_date(start_dt,'dd-mm-yy'), trunc(add_months( to_date(start_dt,'dd-mm-yy'), 3 * (level - 1)), 'Q')), 'YYYY') as year
    from dual
    connect by level <= trunc(months_between(trunc( to_date(end_dt,'dd-mm-yy'), 'Q'), trunc( to_date(start_dt,'dd-mm-yy'), 'Q'))) / 3 + 1;
    v_startquarter date;
    v_endquarter date; 
    v_quarter numeric; 
    v_year numeric;
    v_query varchar2(10000);
       
        BEGIN
            OPEN fct_sales_curs;
        
        LOOP
            FETCH fct_sales_curs INTO v_startquarter, v_endquarter, v_quarter, v_year;
            exit when fct_sales_curs%notfound;
            log_writer('MSG', obj_name, sysdate, null, 'Truncate table WRK_EXCHANGE_SALES');
            
            --truncating table
            execute immediate 'Truncate table WRK_EXCHANGE_SALES';
            
            log_writer('MSG', obj_name, sysdate, null, 'Insert q'||v_quarter||'_'||v_year||' into wrk_sales_exchange');                       

				--inserting
				INSERT INTO WRK_EXCHANGE_SALES
                            (customer_surr_id,
                            DELIVERY_ADDRESS_SURR_ID,
                            EMPLOYEE_SURR_ID,
                            PRODUCT_SURR_ID,
                            CHANNEL_SURR_ID,
                            PAYMENT_TYPE_SURR_ID,
                            date_id,
                            revenue,
                            unit_price,
                            unit_cost,
                            quantity,
                            total_price,
                            UPDATE_DT,
                            INSERT_DT )
				SELECT  nvl(dim_c.customer_surr_id, -99) as customer_surr_id,
                        nvl(dim_da.DELIVERY_ADDRESS_SURR_ID, -99) as DELIVERY_ADDRESS_SURR_ID,
                        nvl(dim_e.EMPLOYEE_SURR_ID, -99) as EMPLOYEE_SURR_ID,
                        nvl(dim_p.PRODUCT_SURR_ID, -99) as PRODUCT_SURR_ID,
                        nvl(dim_ch.CHANNEL_SURR_ID, -99) as CHANNEL_SURR_ID,
                        nvl(dim_pt.PAYMENT_TYPE_SURR_ID, -99) as PAYMENT_TYPE_SURR_ID,
                        to_date(s.transaction_date, 'dd-mm-yy'),
                       round((s.unity_price - s.unity_cost) * s.quantity,2),
						s.unity_price,
                        s.unity_cost,
                        s.quantity,
						round(s.unity_price*s.quantity,2),
						SYSDATE,
						SYSDATE
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
                
                where to_date(transaction_date, 'dd-mm-yy') between v_startquarter and v_endquarter;
           
               	log_writer('MSG', obj_name, sysdate, null,'Finish insert ' || SQL%ROWCOUNT || ' rows');
				
				COMMIT;
                v_query := 'alter table bl_dm.fct_sales
            exchange partition q'
            ||v_quarter
            ||'_'
            ||v_year
            ||'
            with table bl_cl.WRK_EXCHANGE_SALES 
            including indexes without validation update global indexes';
           log_writer('MSG', obj_name, sysdate, null, 'Exchange partition');
            
            execute immediate v_query;
				
            END LOOP;

             CLOSE fct_sales_curs;
            

		EXCEPTION
			WHEN OTHERS THEN
				log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
            
    END etl_fct_sales;


END pkg_etl_fct_sales;

exec pkg_etl_fct_sales.ld_wrk_sales;
exec pkg_etl_fct_sales.etl_ce_sales;
exec pkg_etl_fct_sales.etl_fct_sales('01-01-19','01-10-21');