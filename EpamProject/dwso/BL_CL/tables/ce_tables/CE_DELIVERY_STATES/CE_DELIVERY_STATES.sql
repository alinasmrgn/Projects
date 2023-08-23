-- ce_delivery_states
MERGE INTO ce_delivery_states ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					DELIVERY_STATE_ID                      AS source_id,
					DELIVERY_STATE                         AS DELIVERY_STATE,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT da.DELIVERY_STATE_ID                            AS DELIVERY_STATE_ID,
                                        COALESCE(UPPER(da.DELIVERY_STATE),'N/A')        AS DELIVERY_STATE
						FROM sa_instore.sa_delivery_address da
                    ) cc
        )src
 ON  (
		ce.DELIVERY_STATE_SRC_ID = to_char(src.source_id)                AND 
		ce.DELIVERY_STATE_SOURCE_SYSTEM = src.source_system     AND
		ce.DELIVERY_STATE_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.DELIVERY_STATE = src.DELIVERY_STATE,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.DELIVERY_STATE,ce.DELIVERY_STATE,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.DELIVERY_STATE_ID,ce.DELIVERY_STATE_SRC_ID,ce.DELIVERY_STATE_SOURCE_SYSTEM,ce.DELIVERY_STATE_SOURCE_TABLE,ce.DELIVERY_STATE,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_DELIVERY_STATES_S.nextval,src.source_id,src.source_system,src.source_table,src.DELIVERY_STATE,src.insert_dt,src.update_dt);

delete from ce_delivery_states;
select * from ce_delivery_states
