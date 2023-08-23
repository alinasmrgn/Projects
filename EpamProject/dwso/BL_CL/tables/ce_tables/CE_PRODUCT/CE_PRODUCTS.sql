-- ce_products
MERGE INTO ce_products ce
USING   (
			SELECT  'SA_BICYCLE'                         AS source_system,
					'SA_INSTORE'                         AS source_table,
					PRODUCT_SURR_ID                      AS source_id,
					PRODUCT_BRAND_ID                 AS PRODUCT_BRAND_ID,
                    PRODUCT_LINE                         AS PRODUCT_LINE,
                    PRODUCT_SIZE                         AS PRODUCT_SIZE,
                    PRODUCT_CLASS                        AS PRODUCT_CLASS,
					SYSDATE                              AS insert_dt,
					SYSDATE                              AS update_dt
			FROM    (
						SELECT DISTINCT sc.PRODUCT_SURR_ID                            AS PRODUCT_SURR_ID,
                                        COALESCE(nf3.PRODUCT_BRAND_ID,-99)        AS PRODUCT_BRAND_ID,
                                        COALESCE(UPPER(sc.PRODUCT_LINE),'N/A')        AS PRODUCT_LINE,
                                        COALESCE(UPPER(sc.PRODUCT_SIZE),'N/A')        AS PRODUCT_SIZE,
                                        COALESCE(UPPER(sc.PRODUCT_CLASS),'N/A')        AS PRODUCT_CLASS
						FROM sa_instore.sa_products sc
                        LEFT OUTER JOIN ce_product_brands nf3 on sc.PRODUCT_BRAND_ID = nf3.PRODUCT_BRAND_SRC_ID AND nf3.PRODUCT_BRAND_SOURCE_SYSTEM = 'SA_BICYCLE' AND nf3.PRODUCT_BRAND_SOURCE_TABLE = 'SA_INSTORE'
                    ) cc
        )src
 ON  (
		ce.PRODUCT_SRC_ID = to_char(src.source_id)                 AND 
		ce.PRODUCT_SOURCE_SYSTEM = src.source_system     AND
		ce.PRODUCT_SOURCE_TABLE = src.source_table
     )
WHEN MATCHED THEN UPDATE SET    ce.PRODUCT_BRAND_ID = src.PRODUCT_BRAND_ID,
                                ce.PRODUCT_LINE = src.PRODUCT_LINE,
                                ce.PRODUCT_SIZE = src.PRODUCT_SIZE,
                                ce.PRODUCT_CLASS = src.PRODUCT_CLASS,
                                ce.update_dt = SYSDATE
WHERE DECODE(src.PRODUCT_BRAND_ID,ce.PRODUCT_BRAND_ID,0,1) + 
    DECODE(src.PRODUCT_LINE,ce.PRODUCT_LINE,0,1) +
    DECODE(src.PRODUCT_SIZE,ce.PRODUCT_SIZE,0,1) +
    DECODE(src.PRODUCT_CLASS,ce.PRODUCT_CLASS,0,1) > 0 
WHEN NOT MATCHED THEN   INSERT (ce.PRODUCT_ID,ce.PRODUCT_SRC_ID,ce.PRODUCT_SOURCE_SYSTEM,ce.PRODUCT_SOURCE_TABLE,ce.PRODUCT_BRAND_ID,ce.PRODUCT_LINE,ce.PRODUCT_SIZE,ce.PRODUCT_CLASS,ce.insert_dt,ce.update_dt) 
                        VALUES (CE_PRODUCTS_S.nextval,src.source_id,src.source_system,src.source_table,src.PRODUCT_BRAND_ID, src.PRODUCT_LINE,src.PRODUCT_SIZE, src.PRODUCT_CLASS,src.insert_dt,src.update_dt);

delete from ce_products ;
select * from ce_products;
