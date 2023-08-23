-- CE_CHANNELS_CLASSES
MERGE INTO ce_channels_classes ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					channel_class_id                      AS source_id,
					channel_class                         AS channel_class,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT channel_class_id                            AS channel_class_id,
										COALESCE(UPPER(channel_class),'N/A')        AS channel_class 
						FROM sa_instore.sa_channels
                    
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


delete from ce_channels_classes;
select * from ce_channels_classes
