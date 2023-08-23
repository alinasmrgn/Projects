-- ce_customer_job_categories
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

delete from ce_customer_job_categories;
select * from ce_customer_job_categories