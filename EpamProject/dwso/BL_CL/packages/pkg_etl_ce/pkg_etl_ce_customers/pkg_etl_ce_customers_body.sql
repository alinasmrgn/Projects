
CREATE OR REPLACE PACKAGE BODY pkg_etl_ce_customers
AS
    PROCEDURE etl_ce_customers AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_customers');
        Merge into ce_customers ce
    Using ( 
            With all_cust as                            -- all customers from two systems
            (Select  CUSTOMER_SURR_ID as scr_id,
                    'SA_CUSTOMER_INSTORE' as scr_table,
                    'SA_INSTORE' as scr_system,
                    CUSTOMER_FIRST_NAME,
                    CUSTOMER_LAST_NAME,
                    CUSTOMER_GENDER,
                    TO_DATE(CUSTOMER_DOB, 'YYYY-MM-DD') as birthday, 
                    CUSTOMER_WEALTH_SEGMENT,
                    CUSTOMER_JOB_ID
            FROM sa_instore.sa_customer_instore
            UNION ALL     
            Select  CUSTOMER_SURR_ID as scr_id,
                    'SA_CUSTOMER_ONLINE' as scr_table,
                    'SA_ONLINE' as scr_system,               
                    CUSTOMER_FIRST_NAME,
                    CUSTOMER_LAST_NAME,
                    CUSTOMER_GENDER,
                    TO_DATE(CUSTOMER_DOB, 'YYYY-MM-DD') as birthday, 
                    CUSTOMER_WEALTH_SEGMENT,
                    CUSTOMER_JOB_ID                        
            FROM sa_online.sa_customer_online),
            
            distinct_map_cust as                    -- distinct customers from map_ce_customers
            (Select     customer_id,
                        customer_scr_id, 
                        customer_scr_table,
                        customer_scr_system,
                        CUSTOMER_FIRST_NAME, 
                        CUSTOMER_LAST_NAME, 
                        CUSTOMER_GENDER, 
                        CUSTOMER_DOB,
                        CUSTOMER_WEALTH_SEGMENT, 
                        CUSTOMER_JOB_ID
                        
            From   (Select mp.*, row_number() over(partition by customer_id order by customer_scr_system) as rnum  -- rely on the 1st system = sa_instore
                    From map_ce_customers mp)
                Where rnum = 1)
                
            Select  dmc.customer_id as scr_id,      -- join two cte to find full information about the customer
                    'BL_CL' as scr_system,
                    'MAP_CE_CUSTOMERS' as scr_table,
                    dmc.CUSTOMER_FIRST_NAME,
                    dmc.CUSTOMER_LAST_NAME,
                    all_cust.CUSTOMER_GENDER,
                    dmc.CUSTOMER_DOB,
                    all_cust.CUSTOMER_WEALTH_SEGMENT,
                    all_cust.CUSTOMER_JOB_ID
            From distinct_map_cust dmc
            Inner join all_cust on dmc.customer_scr_id = all_cust.scr_id
                                and dmc.customer_scr_table = all_cust.scr_table
                                and dmc.customer_scr_system = all_cust.scr_system              
         ) sa
         
ON (
        ce.customer_src_id = to_char(sa.scr_id)
        and ce.customer_source_table = sa.scr_table
        and ce.customer_source_system = sa.scr_system
    )
When matched then
    Update set  ce.CUSTOMER_FIRST_NAME = sa.CUSTOMER_FIRST_NAME,              
                ce.CUSTOMER_LAST_NAME  = sa.CUSTOMER_LAST_NAME,             
                ce.CUSTOMER_GENDER =  sa.CUSTOMER_GENDER,
                ce.CUSTOMER_DOB = sa.CUSTOMER_DOB,           
                ce.CUSTOMER_WEALTH_SEGMENT =  sa.CUSTOMER_WEALTH_SEGMENT,  
                ce.CUSTOMER_JOB_ID =  sa.CUSTOMER_JOB_ID,  
                ce.update_dt = sysdate
    Where   decode(ce.CUSTOMER_FIRST_NAME, sa.CUSTOMER_FIRST_NAME, 0, 1) + 
            decode(ce.CUSTOMER_LAST_NAME, sa.CUSTOMER_LAST_NAME, 0, 1) + 
            decode(ce.CUSTOMER_GENDER, sa.CUSTOMER_GENDER, 0, 1) + 
            decode(ce.CUSTOMER_DOB, sa.CUSTOMER_DOB, 0, 1) + 
            decode(ce.CUSTOMER_WEALTH_SEGMENT, sa.CUSTOMER_WEALTH_SEGMENT, 0, 1) + 
            decode(ce.CUSTOMER_JOB_ID, sa.CUSTOMER_JOB_ID, 0, 1) > 0
When not matched then
    Insert (customer_id,
            customer_src_id,
            customer_source_table,
            customer_source_system,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            CUSTOMER_DOB,
            CUSTOMER_WEALTH_SEGMENT,
            CUSTOMER_JOB_ID,
            insert_dt,
            update_dt)
            
    Values (CE_CUSTOMERS_S.nextval,
            sa.scr_id,
            sa.scr_table,
            sa.scr_system,
            sa.CUSTOMER_FIRST_NAME,
            sa.CUSTOMER_LAST_NAME,
            sa.CUSTOMER_GENDER,
            sa.CUSTOMER_DOB,
            sa.CUSTOMER_WEALTH_SEGMENT,
            sa.CUSTOMER_JOB_ID,
            sysdate,
            sysdate);       
        
        COMMIT;        
        log_writer('MSG', obj_name, sysdate, null, 'Merge into ce_customers ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_customers;
     
    -------------------------------------------------------------------------------------------------     
      -- SOURCE1: SA_CUSTOMER_INSTORE
    PROCEDURE etl_map_ce_customers_instore AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into map_ce_customers_instore');
        Merge into map_ce_customers mp
Using (
        With cust as        
        (Select CUSTOMER_FIRST_NAME,
                CUSTOMER_LAST_NAME,
                CUSTOMER_GENDER,
                TO_DATE(CUSTOMER_DOB, 'YYYY-MM-DD') as birthday,      
                CUSTOMER_WEALTH_SEGMENT,
                CUSTOMER_JOB_ID,
                CUSTOMER_SURR_ID as scr_id                      
        FROM sa_instore.sa_customer_instore)
        
        Select  mp1.customer_id as exist_cust_id,          
                cust.CUSTOMER_FIRST_NAME,
                cust.CUSTOMER_LAST_NAME,
                cust.CUSTOMER_GENDER,
                cust.birthday,
                cust.CUSTOMER_WEALTH_SEGMENT,
                nf3.CUSTOMER_JOB_ID,
                cust.scr_id
        From cust
        Left Join map_ce_customers mp1 on   mp1.CUSTOMER_FIRST_NAME = cust.CUSTOMER_FIRST_NAME
                                            and mp1.CUSTOMER_LAST_NAME = cust.CUSTOMER_LAST_NAME  -- check if there is such a combination: name+birthday
                                            and mp1.CUSTOMER_DOB = cust.birthday
        Left Join ce_customer_jobs nf3 on   nf3.CUSTOMER_JOB_src_ID = to_char(cust.CUSTOMER_JOB_ID)
                                            
      ) sa
ON (
        mp.customer_scr_id = sa.scr_id
        and mp.customer_scr_table = 'SA_CUSTOMER_INSTORE'
        and mp.customer_scr_system = 'SA_INSTORE'
    )
When matched then 
    Update set  mp.CUSTOMER_FIRST_NAME = sa.CUSTOMER_FIRST_NAME,
                mp.CUSTOMER_LAST_NAME = sa.CUSTOMER_LAST_NAME,
                mp.CUSTOMER_GENDER = sa.CUSTOMER_GENDER,
                mp.CUSTOMER_DOB = sa.birthday,
                mp.CUSTOMER_WEALTH_SEGMENT = sa.CUSTOMER_WEALTH_SEGMENT,
                mp.CUSTOMER_JOB_ID = sa.CUSTOMER_JOB_ID
    Where   decode(mp.CUSTOMER_FIRST_NAME, sa.CUSTOMER_FIRST_NAME, 0, 1) + 
            decode(mp.CUSTOMER_LAST_NAME, sa.CUSTOMER_LAST_NAME, 0, 1) +
            decode(mp.CUSTOMER_GENDER, sa.CUSTOMER_GENDER, 0, 1) +
            decode(mp.CUSTOMER_DOB, sa.birthday, 0, 1) + 
            decode(mp.CUSTOMER_WEALTH_SEGMENT, sa.CUSTOMER_WEALTH_SEGMENT, 0, 1) +
            decode(mp.CUSTOMER_JOB_ID, sa.CUSTOMER_JOB_ID, 0, 1)  > 0
When not matched then
    Insert (customer_id,
            customer_scr_id,
            customer_scr_table,
            customer_scr_system,   
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            CUSTOMER_DOB,
            CUSTOMER_WEALTH_SEGMENT,
            CUSTOMER_JOB_ID)            
    Values (nvl(sa.exist_cust_id, map_ce_customers_seq.nextval), -- generate id or insert existing id (id name+birthday+loaylty_card matched above)
            sa.scr_id,
            'SA_CUSTOMER_INSTORE',
            'SA_INSTORE',
            sa.CUSTOMER_FIRST_NAME, 
            sa.CUSTOMER_LAST_NAME, 
            sa.CUSTOMER_GENDER, 
            sa.birthday,
            sa.CUSTOMER_WEALTH_SEGMENT,
            sa.CUSTOMER_JOB_ID);        
        
        log_writer('MSG', obj_name, sysdate, null, 'Merge into map_ce_customers_instore ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_map_ce_customers_instore;
     
    -------------------------------------------------------------------------------------------------    
    -- SOURCE2: SA_CUSTOMER_ONLINE
    PROCEDURE etl_map_ce_customers_online AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into map_ce_customers_online');
        Merge into map_ce_customers mp
Using (
        With cust as        
        (Select CUSTOMER_FIRST_NAME,
                CUSTOMER_LAST_NAME,
                CUSTOMER_GENDER,
                TO_DATE(CUSTOMER_DOB, 'YYYY-MM-DD') as birthday,      
                CUSTOMER_WEALTH_SEGMENT,
                CUSTOMER_JOB_ID,
                CUSTOMER_SURR_ID as scr_id                      
        FROM sa_online.sa_customer_online)
        
        Select  mp1.customer_id as exist_cust_id,          
                cust.CUSTOMER_FIRST_NAME,
                cust.CUSTOMER_LAST_NAME,
                cust.CUSTOMER_GENDER,
                cust.birthday,
                cust.CUSTOMER_WEALTH_SEGMENT,
                nf3.CUSTOMER_JOB_ID,
                cust.scr_id
        From cust
        Left Join map_ce_customers mp1 on   mp1.CUSTOMER_FIRST_NAME = cust.CUSTOMER_FIRST_NAME
                                            and mp1.CUSTOMER_LAST_NAME = cust.CUSTOMER_LAST_NAME  -- check if there is such a combination: name+birthday
                                            and mp1.CUSTOMER_DOB = cust.birthday
        Left Join ce_customer_jobs nf3 on   nf3.CUSTOMER_JOB_src_ID = to_char(cust.CUSTOMER_JOB_ID)
                                            
      ) sa
ON (
        mp.customer_scr_id = sa.scr_id
        and mp.customer_scr_table = 'SA_CUSTOMER_ONLINE'
        and mp.customer_scr_system = 'SA_ONLINE'
    )
When matched then 
    Update set  mp.CUSTOMER_FIRST_NAME = sa.CUSTOMER_FIRST_NAME,
                mp.CUSTOMER_LAST_NAME = sa.CUSTOMER_LAST_NAME,
                mp.CUSTOMER_GENDER = sa.CUSTOMER_GENDER,
                mp.CUSTOMER_DOB = sa.birthday,
                mp.CUSTOMER_WEALTH_SEGMENT = sa.CUSTOMER_WEALTH_SEGMENT,
                mp.CUSTOMER_JOB_ID = sa.CUSTOMER_JOB_ID
    Where   decode(mp.CUSTOMER_FIRST_NAME, sa.CUSTOMER_FIRST_NAME, 0, 1) + 
            decode(mp.CUSTOMER_LAST_NAME, sa.CUSTOMER_LAST_NAME, 0, 1) +
            decode(mp.CUSTOMER_GENDER, sa.CUSTOMER_GENDER, 0, 1) +
            decode(mp.CUSTOMER_DOB, sa.birthday, 0, 1) + 
            decode(mp.CUSTOMER_WEALTH_SEGMENT, sa.CUSTOMER_WEALTH_SEGMENT, 0, 1) +
            decode(mp.CUSTOMER_JOB_ID, sa.CUSTOMER_JOB_ID, 0, 1)  > 0
When not matched then
    Insert (customer_id,
            customer_scr_id,
            customer_scr_table,
            customer_scr_system,   
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_GENDER,
            CUSTOMER_DOB,
            CUSTOMER_WEALTH_SEGMENT,
            CUSTOMER_JOB_ID)            
    Values (nvl(sa.exist_cust_id, map_ce_customers_seq.nextval), -- generate id or insert existing id (id name+birthday+loaylty_card matched above)
            sa.scr_id,
            'SA_CUSTOMER_ONLINE',
            'SA_ONLINE',
            sa.CUSTOMER_FIRST_NAME, 
            sa.CUSTOMER_LAST_NAME, 
            sa.CUSTOMER_GENDER, 
            sa.birthday,
            sa.CUSTOMER_WEALTH_SEGMENT,
            sa.CUSTOMER_JOB_ID);
        
        log_writer('MSG', obj_name, sysdate, null, 'Merge into map_ce_customers_online ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_map_ce_customers_online;

    -------------------------------------------------------------------------------------------------    
    --  SA_CUSTOMER_JOBS
    PROCEDURE etl_ce_customer_jobs AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_customer_jobs');
        MERGE INTO ce_customer_jobs ce
USING   (
			SELECT  'SA_BICYCLE'                    AS source_system,
					'SA_INSTORE'                    AS source_table,
					CUSTOMER_JOB_ID                 AS source_id,
					CUSTOMER_JOB_TITLE              AS CUSTOMER_JOB_TITLE,
					CUSTOMER_JOB_CATEGORY_ID        AS CUSTOMER_JOB_CATEGORY_ID,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT pt.CUSTOMER_JOB_ID                            AS CUSTOMER_JOB_ID,
                                        COALESCE(UPPER(pt.CUSTOMER_JOB_TITLE),'N/A')        AS CUSTOMER_JOB_TITLE,
                                        COALESCE(pt.CUSTOMER_JOB_CATEGORY_ID,-99)        AS CUSTOMER_JOB_CATEGORY_ID
						FROM sa_instore.sa_customer_jobs pt
                        LEFT OUTER JOIN ce_customer_job_categories nf3 on pt.CUSTOMER_JOB_CATEGORY_ID = nf3.CUSTOMER_JOB_CATEGORY_src_ID AND nf3.CUSTOMER_JOB_CATEGORY_SOURCE_SYSTEM = 'SA_BICYCLE' AND nf3.CUSTOMER_JOB_CATEGORY_SOURCE_TABLE = 'SA_INSTORE'
                    ) cc
        )src
 ON  (
		ce.CUSTOMER_JOB_SRC_ID = to_char(src.source_id)                AND 
		ce.CUSTOMER_JOB_SOURCE_SYSTEM = src.source_system     AND
		ce.CUSTOMER_JOB_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.CUSTOMER_JOB_TITLE = src.CUSTOMER_JOB_TITLE,
                                ce.CUSTOMER_JOB_CATEGORY_ID = src.CUSTOMER_JOB_CATEGORY_ID,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.CUSTOMER_JOB_TITLE,ce.CUSTOMER_JOB_TITLE,0,1) + DECODE(src.CUSTOMER_JOB_CATEGORY_ID,ce.CUSTOMER_JOB_CATEGORY_ID,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.CUSTOMER_JOB_ID,ce.CUSTOMER_JOB_SRC_ID,ce.CUSTOMER_JOB_SOURCE_SYSTEM,ce.CUSTOMER_JOB_SOURCE_TABLE,ce.CUSTOMER_JOB_TITLE,ce.CUSTOMER_JOB_CATEGORY_ID,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_CUSTOMER_JOBS_S.nextval,src.source_id,src.source_system,src.source_table,src.CUSTOMER_JOB_TITLE,src.CUSTOMER_JOB_CATEGORY_ID,src.insert_dt,src.update_dt);

     COMMIT;     
        log_writer('MSG', obj_name, sysdate, null, 'Merge into ce_customer_jobs ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_customer_jobs;
    -------------------------------------------------------------------------------------------------    
    --  SA_CUSTOMER_JOBS_CATEGORIES
    PROCEDURE etl_ce_customer_job_categories AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_customer_job_categories');
        MERGE INTO ce_customer_job_categories ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					CUSTOMER_JOB_CATEGORY_ID                      AS source_id,
					CUSTOMER_JOB_INDUSTRY_CATEGORY                         AS CUSTOMER_JOB_INDUSTRY_CATEGORY,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT pt.CUSTOMER_JOB_CATEGORY_ID                            AS CUSTOMER_JOB_CATEGORY_ID,
                                        COALESCE(UPPER(pt.CUSTOMER_JOB_INDUSTRY_CATEGORY),'N/A')        AS CUSTOMER_JOB_INDUSTRY_CATEGORY
						FROM sa_instore.sa_customer_job_categories pt
                    ) cc
        )src
 ON  (
		ce.CUSTOMER_JOB_CATEGORY_SRC_ID = to_char(src.source_id)                AND 
		ce.CUSTOMER_JOB_CATEGORY_SOURCE_SYSTEM = src.source_system     AND
		ce.CUSTOMER_JOB_CATEGORY_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.CUSTOMER_JOB_CATEGORY = src.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.CUSTOMER_JOB_INDUSTRY_CATEGORY,ce.CUSTOMER_JOB_CATEGORY,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.CUSTOMER_JOB_CATEGORY_ID,ce.CUSTOMER_JOB_CATEGORY_SRC_ID,ce.CUSTOMER_JOB_CATEGORY_SOURCE_SYSTEM,ce.CUSTOMER_JOB_CATEGORY_SOURCE_TABLE,ce.CUSTOMER_JOB_CATEGORY,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_CUSTOMER_JOB_CATEGORIES_S.nextval,src.source_id,src.source_system,src.source_table,src.CUSTOMER_JOB_INDUSTRY_CATEGORY,src.insert_dt,src.update_dt);

     COMMIT;     
        log_writer('MSG', obj_name, sysdate, null, 'Merge into ce_customer_job_categories ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_customer_job_categories;
        
END pkg_etl_ce_customers;


-- TRY
exec pkg_etl_ce_customers.etl_ce_customers;

