 Merge into dim_customers dm
            Using ( 
                    Select  'BL_3NF' as scr_system,
                            'CE_CUSTOMERS' as scr_table,
                            c.customer_id as scr_id,
                            c.CUSTOMER_FIRST_NAME,
                            c.CUSTOMER_LAST_NAME,
                            c.CUSTOMER_GENDER, 
                            c.CUSTOMER_DOB, 
                            nvl(cj.CUSTOMER_JOB_ID,-99) as CUSTOMER_JOB_ID,
                            cj.CUSTOMER_JOB_TITLE,
                            nvl(cjc.CUSTOMER_JOB_CATEGORY_ID,-99) as CUSTOMER_JOB_INDUSTRY_CATEGORY_ID,
                            cjc.CUSTOMER_JOB_CATEGORY as CUSTOMER_JOB_INDUSTRY_CATEGORY,
                            c.CUSTOMER_WEALTH_SEGMENT
                    From ce_customers c
                    Left join ce_customer_jobs cj on c.CUSTOMER_JOB_ID = cj.CUSTOMER_JOB_ID
                    Left join ce_customer_job_categories cjc on cj.CUSTOMER_JOB_CATEGORY_ID = cjc.CUSTOMER_JOB_CATEGORY_ID
                    Where c.customer_id > 0
                  ) ce
        On (dm.customer_surr_id = to_char(ce.scr_id))
        When matched then 
            Update set  dm.CUSTOMER_FIRST_NAME = ce.CUSTOMER_FIRST_NAME,
                        dm.CUSTOMER_LAST_NAME = ce.CUSTOMER_LAST_NAME,
                        dm.CUSTOMER_GENDER = ce.CUSTOMER_GENDER,
                        dm.CUSTOMER_DOB = ce.CUSTOMER_DOB,
                        dm.CUSTOMER_JOB_ID = ce.CUSTOMER_JOB_ID,
                        dm.CUSTOMER_JOB_TITLE = ce.CUSTOMER_JOB_TITLE,
                        dm.CUSTOMER_JOB_INDUSTRY_CATEGORY_ID = ce.CUSTOMER_JOB_INDUSTRY_CATEGORY_ID,
                        dm.CUSTOMER_JOB_INDUSTRY_CATEGORY = ce.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                        dm.CUSTOMER_WEALTH_SEGMENT = ce.CUSTOMER_WEALTH_SEGMENT,
                        dm.update_dt = sysdate
            Where   decode(dm.CUSTOMER_FIRST_NAME, ce.CUSTOMER_FIRST_NAME, 0, 1) + 
                    decode(dm.CUSTOMER_LAST_NAME, ce.CUSTOMER_LAST_NAME, 0, 1) + 
                    decode(dm.CUSTOMER_GENDER, ce.CUSTOMER_GENDER, 0, 1) + 
                    decode(dm.CUSTOMER_DOB, ce.CUSTOMER_DOB, 0, 1) + 
                    decode(dm.CUSTOMER_JOB_ID, ce.CUSTOMER_JOB_ID, 0, 1) + 
                    decode(dm.CUSTOMER_JOB_TITLE, ce.CUSTOMER_JOB_TITLE, 0, 1)+
                    decode(dm.CUSTOMER_JOB_INDUSTRY_CATEGORY_ID, ce.CUSTOMER_JOB_INDUSTRY_CATEGORY_ID, 0, 1) + 
                    decode(dm.CUSTOMER_JOB_INDUSTRY_CATEGORY, ce.CUSTOMER_JOB_INDUSTRY_CATEGORY, 0, 1) + 
                    decode(dm.CUSTOMER_WEALTH_SEGMENT, ce.CUSTOMER_WEALTH_SEGMENT, 0, 1) > 0
        When not matched then 
            Insert (customer_surr_id,
                    source_system,
                    source_table,
                    source_id,
                    CUSTOMER_FIRST_NAME,
                    CUSTOMER_LAST_NAME,
                    CUSTOMER_GENDER,
                    CUSTOMER_DOB,
                    CUSTOMER_JOB_TITLE,
                    CUSTOMER_JOB_ID,
                    CUSTOMER_JOB_INDUSTRY_CATEGORY,
                    CUSTOMER_JOB_INDUSTRY_CATEGORY_ID,
                    CUSTOMER_WEALTH_SEGMENT,
                    insert_dt,
                    update_dt)
            Values (DIM_CUSTOMERS_S.nextval,
                    ce.scr_system,
                    ce.scr_table,
                    ce.scr_id,
                    ce.CUSTOMER_FIRST_NAME, 
                    ce.CUSTOMER_LAST_NAME, 
                    ce.CUSTOMER_GENDER, 
                    ce.CUSTOMER_DOB,
                    ce.CUSTOMER_JOB_TITLE, 
                    ce.CUSTOMER_JOB_ID, 
                    ce.CUSTOMER_JOB_INDUSTRY_CATEGORY,
                    ce.CUSTOMER_JOB_INDUSTRY_CATEGORY_ID, 
                    ce.CUSTOMER_WEALTH_SEGMENT,
                    sysdate,
                    sysdate);           
commit;   