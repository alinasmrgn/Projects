-- ce_delivery_addresses
MERGE INTO ce_delivery_addresses ce
USING   (
			SELECT  'SA_BICYCLE'                    AS source_system,
					'SA_INSTORE'                    AS source_table,
					DELIVERY_ADDRESS_SURR_ID        AS source_id,
					DELIVERY_STREET_ADDRESS         AS DELIVERY_STREET_ADDRESS,
                    DELIVERY_POSTAL_CODE            AS DELIVERY_POSTAL_CODE,
                    DELIVERY_STATE_ID               AS DELIVERY_STATE_ID,
                    DELIVERY_COUNTRY                AS DELIVERY_COUNTRY,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT da.DELIVERY_ADDRESS_SURR_ID                            AS DELIVERY_ADDRESS_SURR_ID,
                                        COALESCE(UPPER(da.DELIVERY_STREET_ADDRESS),'N/A')        AS DELIVERY_STREET_ADDRESS,
                                        COALESCE(da.DELIVERY_POSTAL_CODE,-99)        AS DELIVERY_POSTAL_CODE,
                                        COALESCE(nf3.DELIVERY_STATE_ID,-99)        AS DELIVERY_STATE_ID,
                                        COALESCE(UPPER(da.DELIVERY_COUNTRY),'N/A')        AS DELIVERY_COUNTRY
						FROM sa_instore.sa_delivery_address da
                        LEFT OUTER JOIN ce_delivery_states nf3 on da.DELIVERY_STATE_ID = nf3.DELIVERY_STATE_src_ID AND nf3.DELIVERY_STATE_SOURCE_SYSTEM = 'SA_BICYCLE' AND nf3.DELIVERY_STATE_SOURCE_TABLE = 'SA_INSTORE'
                    ) cc
        )src
 ON  (
		ce.DELIVERY_ADDRESS_SRC_ID = to_char(src.source_id)                AND 
		ce.DELIVERY_ADDRESS_SOURCE_SYSTEM = src.source_system     AND
		ce.DELIVERY_ADDRESS_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.DELIVERY_STREET_ADDRESS = src.DELIVERY_STREET_ADDRESS,
                                ce.DELIVERY_POSTAL_CODE = src.DELIVERY_POSTAL_CODE,
                                ce.DELIVERY_STATE_ID = src.DELIVERY_STATE_ID,
                                ce.DELIVERY_COUNTRY = src.DELIVERY_COUNTRY,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.DELIVERY_STREET_ADDRESS,ce.DELIVERY_STREET_ADDRESS,0,1) + 
        DECODE(src.DELIVERY_POSTAL_CODE,ce.DELIVERY_POSTAL_CODE,0,1) +
        DECODE(src.DELIVERY_STATE_ID,ce.DELIVERY_STATE_ID,0,1) +
        DECODE(src.DELIVERY_COUNTRY,ce.DELIVERY_COUNTRY,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.DELIVERY_ADDRESS_ID,ce.DELIVERY_ADDRESS_SRC_ID,ce.DELIVERY_ADDRESS_SOURCE_SYSTEM,ce.DELIVERY_ADDRESS_SOURCE_TABLE,ce.DELIVERY_STREET_ADDRESS,ce.DELIVERY_POSTAL_CODE ,ce.DELIVERY_STATE_ID,ce.DELIVERY_COUNTRY ,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_DELIVERY_ADDRESSES_S.nextval,src.source_id,src.source_system,src.source_table,src.DELIVERY_STREET_ADDRESS,src.DELIVERY_POSTAL_CODE,src.DELIVERY_STATE_ID,src.DELIVERY_COUNTRY,src.insert_dt,src.update_dt);

delete from ce_delivery_addresses;
select * from ce_delivery_addresses order by delivery_address_id 