-- ce_payment_types
MERGE INTO ce_payment_types ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					PAYMENT_TYPE_SURR_ID                      AS source_id,
					PAYMENT_TYPE_NAME                         AS PAYMENT_TYPE_NAME,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT pt.PAYMENT_TYPE_SURR_ID                            AS PAYMENT_TYPE_SURR_ID,
                                        COALESCE(UPPER(pt.PAYMENT_TYPE_NAME),'N/A')        AS PAYMENT_TYPE_NAME
						FROM sa_instore.sa_payment_types pt
                    ) cc
        )src
 ON  (
		ce.PAYMENT_TYPE_SRC_ID = to_char(src.source_id)                AND 
		ce.PAYMENT_TYPE_SOURCE_SYSTEM = src.source_system     AND
		ce.PAYMENT_TYPE_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.PAYMENT_TYPE_NAME = src.PAYMENT_TYPE_NAME,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.PAYMENT_TYPE_NAME,ce.PAYMENT_TYPE_NAME,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.PAYMENT_TYPE_ID,ce.PAYMENT_TYPE_SRC_ID,ce.PAYMENT_TYPE_SOURCE_SYSTEM,ce.PAYMENT_TYPE_SOURCE_TABLE,ce.PAYMENT_TYPE_NAME,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_PAYMENT_TYPES_S.nextval,src.source_id,src.source_system,src.source_table,src.PAYMENT_TYPE_NAME,src.insert_dt,src.update_dt);


select * from ce_payment_types
