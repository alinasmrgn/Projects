-- ce_product_brands
MERGE INTO ce_product_brands ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					PRODUCT_BRAND_ID                      AS source_id,
					PRODUCT_BRAND                         AS PRODUCT_BRAND,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT PRODUCT_BRAND_ID                            AS PRODUCT_BRAND_ID,
										COALESCE(UPPER(PRODUCT_BRAND),'N/A')        AS PRODUCT_BRAND 
						FROM sa_instore.sa_products
                    
                    ) cc
                    
        )src
 ON  (
		ce.PRODUCT_BRAND_SRC_ID = to_char(src.source_id)       AND 
		ce.PRODUCT_BRAND_SOURCE_SYSTEM = src.source_system     AND
		ce.PRODUCT_BRAND_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.PRODUCT_BRAND = src.PRODUCT_BRAND,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.PRODUCT_BRAND,ce.PRODUCT_BRAND,0,1)  > 0 
WHEN NOT MATCHED THEN   INSERT (ce.PRODUCT_BRAND_ID,ce.PRODUCT_BRAND_SRC_ID,ce.PRODUCT_BRAND_SOURCE_SYSTEM,ce.PRODUCT_BRAND_SOURCE_TABLE,ce.PRODUCT_BRAND,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_PRODUCT_BRANDS_S.nextval,src.source_id,src.source_system,src.source_table,src.PRODUCT_BRAND,src.insert_dt,src.update_dt);

delete  from ce_product_brands;
select * from ce_product_brands
