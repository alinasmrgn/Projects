-- CE_CUSTOMERS
-- DEDUBLICATION


-- INSERT INTO MAP TABLE data from Source1
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
            
-- INSERT INTO MAP TABLE data from Source2
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
            
-- INSERT INTO CE_CUSTOMERS
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
delete from map_ce_customers;
delete from ce_customers;
select * from ce_customers;
