CREATE OR REPLACE PACKAGE BODY pkg_etl_ce_channels
AS
    PROCEDURE etl_ce_channels_classes AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); 
    BEGIN    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_channels_classes with cursor');   
        
        for rec in (SELECT channel_class_id, channel_class
                    FROM sa_instore.sa_channels)
        loop
            MERGE INTO ce_channels_classes ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					channel_class_id                      AS source_id,
					channel_class                         AS channel_class,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT rec.channel_class_id                            AS channel_class_id,
										COALESCE(UPPER(rec.channel_class),'N/A')        AS channel_class 
						FROM dual
                    
                    ) cc
        )src
 ON  (
		ce.CHANNELS_CLASS_SRC_ID = to_char(src.source_id)                AND 
		ce.CHANNELS_CLASS_SOURCE_SYSTEM = src.source_system     AND
		ce.CHANNELS_CLASS_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.CHANNELS_CLASS = src.channel_class,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.channel_class,ce.channels_class,0,1)  > 0 
WHEN NOT MATCHED THEN   INSERT (ce.CHANNELS_CLASS_ID,ce.CHANNELS_CLASS_SRC_ID,ce.CHANNELS_CLASS_SOURCE_SYSTEM,ce.CHANNELS_CLASS_SOURCE_TABLE,ce.CHANNELS_CLASS,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_CHANNELS_CLASSES_S.nextval,src.source_id,src.source_system,src.source_table,src.channel_class,src.insert_dt,src.update_dt);
   
          end loop;
        COMMIT;
             
        log_writer('MSG', obj_name, sysdate, null, 'Merge into ce_channels_classes ' || SQL%ROWCOUNT || ' rows.');
        

        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_channels_classes;
    -----------------------------------------------------------------------------------------------------------------------------------------------
    PROCEDURE etl_ce_channels AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN    
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_channels with cursor');   
        
    
            MERGE INTO ce_channels ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					CHANNEL_SURR_ID                      AS source_id,
					CHANNEL_NAME                         AS channel_name,
                    CHANNELS_CLASS_ID                         AS CHANNELS_CLASS_ID,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT sc.CHANNEL_SURR_ID                            AS CHANNEL_SURR_ID,
                                        COALESCE(nf3.CHANNELS_CLASS_ID,-99)        AS CHANNELS_CLASS_ID,
                                        COALESCE(UPPER(sc.CHANNEL_NAME),'N/A')        AS CHANNEL_NAME
						FROM sa_instore.sa_channels sc
                        LEFT OUTER JOIN ce_channels_classes nf3 on to_char(sc.CHANNEL_CLASS_ID) = nf3.CHANNELS_CLASS_src_ID AND nf3.CHANNELS_CLASS_SOURCE_SYSTEM = 'SA_BICYCLE' AND nf3.CHANNELS_CLASS_SOURCE_TABLE = 'SA_INSTORE'
                    ) cc
        )src
 ON  (
		ce.CHANNEL_SRC_ID = to_char(src.source_id)                AND 
		ce.CHANNEL_SOURCE_SYSTEM = src.source_system     AND
		ce.CHANNEL_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.CHANNEL_DESC = src.channel_name,
                                ce.CHANNEL_CLASS_ID = src.channels_class_id,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.channel_name,ce.CHANNEL_DESC,0,1) + DECODE(src.channels_class_id,ce.CHANNEL_CLASS_ID,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.CHANNEL_ID,ce.CHANNEL_SRC_ID,ce.CHANNEL_SOURCE_SYSTEM,ce.CHANNEL_SOURCE_TABLE,ce.CHANNEL_DESC,ce.CHANNEL_CLASS_ID,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_CHANNELS_S.nextval,src.source_id,src.source_system,src.source_table,src.channel_name, src.CHANNELS_CLASS_ID,src.insert_dt,src.update_dt);

   
    
        COMMIT;
             
        log_writer('MSG', obj_name, sysdate, null, 'Merge into ce_channels ' || SQL%ROWCOUNT || ' rows.');
        

        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_channels;
        
END pkg_etl_ce_channels;

exec pkg_etl_ce_channels.etl_ce_channels_classes;



