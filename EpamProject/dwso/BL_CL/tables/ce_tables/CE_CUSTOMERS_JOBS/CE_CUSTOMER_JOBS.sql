-- ce_customer_jobs
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

delete from ce_customer_jobs;
select * from ce_customer_jobs