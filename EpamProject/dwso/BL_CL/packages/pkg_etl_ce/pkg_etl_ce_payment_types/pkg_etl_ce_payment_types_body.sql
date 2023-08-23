
CREATE OR REPLACE PACKAGE BODY pkg_etl_ce_payment_types
AS
    PROCEDURE etl_ce_payment_types AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); 
        
        cursor type_cur IS
            (SELECT DISTINCT payment_type_surr_id, payment_type_name
            FROM sa_instore.sa_payment_types);
            
        payment_type_id number(5);
        payment_type varchar2(50);
    BEGIN  
        OPEN type_cur;
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into ce_payment_types with explicit cursor');
        LOOP
            FETCH type_cur into payment_type_id, payment_type;
            EXIT WHEN type_cur%NOTFOUND;    
            
           MERGE INTO ce_payment_types ce
USING   (
			SELECT  'SA_BICYCLE'                     AS source_system,
					'SA_INSTORE'                     AS source_table,
					payment_type_id                      AS source_id,
					payment_type                         AS PAYMENT_TYPE_NAME,
					SYSDATE                         AS insert_dt,
					SYSDATE                         AS update_dt
			FROM    (
						SELECT DISTINCT payment_type_id                            AS payment_type_id,
                                        COALESCE(UPPER(payment_type),'N/A')        AS payment_type
						FROM dual
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
           
            COMMIT;  
        END LOOP;
        log_writer('MSQ', obj_name, sysdate, null, 'Merge into ce_payment_types ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_payment_types;
        
END pkg_etl_ce_payment_types;

exec pkg_etl_ce_payment_types.etl_ce_payment_types;
