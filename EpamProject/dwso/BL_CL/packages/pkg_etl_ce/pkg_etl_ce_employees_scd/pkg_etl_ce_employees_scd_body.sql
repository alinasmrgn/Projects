
CREATE OR REPLACE PACKAGE BODY pkg_etl_ce_employees_scd
AS
    PROCEDURE etl_wrk_ce_employees_scd AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); -- 1 - depth of the stack. Return only the name of the package + procedure from the stack
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge into wrk_ce_employees_scd');
        
        -- DYNAMIC SQL
        EXECUTE IMMEDIATE 'TRUNCATE TABLE wrk_ce_employees_scd';
        
      INSERT INTO wrk_ce_employees_scd(employee_src_id,
                                employee_first_name,
                                employee_last_name,
                                employee_email,
                                employee_phone)
SELECT  EMPLOYEE_SURR_ID, 
        EMPLOYEE_FIRST_NAME, 
        EMPLOYEE_LAST_NAME,
        employee_email,
        REPLACE(employee_phone, CHR(13), '') AS employee_phone  -- has \n = ascii(13) at the end
        FROM sa_instore.sa_employees ;
        
        log_writer('MSG', obj_name, sysdate, null, 'Merge into wrk_ce_employees_scd ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_wrk_ce_employees_scd;
     
     -------------------------------------------------------------------------------------------------------------------
    PROCEDURE etl_ce_employees_scd_merge_step1 AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); 
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start merge(step1) into ce_employees_scd');
        
       MERGE INTO ce_employees_scd ce
    USING (  
            SELECT employee_src_id,
                   employee_first_name,
                   employee_last_name,
                   employee_email,
                   employee_phone 
            FROM wrk_ce_employees_scd
            ) wrk

ON (ce.EMPLOYEE_SRC_ID = wrk.employee_src_id   
    AND ce.EMPLOYEE_SOURCE_TABLE = 'SA_EMPLOYEES' 
    AND ce.EMPLOYEE_SOURCE_SYSTEM = 'SA_INSTORE')    
WHEN MATCHED  THEN
    
    UPDATE SET  ce.end_dt = sysdate,
                ce.is_active = 0                
    WHERE CE.end_dt = TO_DATE('31.12.9999', 'dd.mm.yyyy')   
          AND   (decode(ce.employee_first_name, wrk.employee_first_name, 0, 1) +  
                 decode(ce.employee_last_name, wrk.employee_last_name, 0, 1) + 
                 decode(ce.employee_email, wrk.employee_email, 0, 1) +  
                 decode(ce.employee_phone, wrk.employee_phone, 0, 1) > 0)

WHEN NOT MATCHED THEN
    INSERT (employee_id,           
            employee_src_id, 
            employee_source_system,
            employee_source_table,   
            employee_first_name,          
            employee_last_name,     
            employee_email,          
            employee_phone,
            start_dt,
            end_dt,
            is_active,
            insert_dt)
            
    VALUES  (CE_EMPLOYEES_S.NEXTVAL, 
            wrk.employee_src_id,
            'SA_INSTORE',
            'SA_EMPLOYEES',
            wrk.employee_first_name ,
            wrk.employee_last_name ,
            wrk.employee_email ,
            wrk.employee_phone,
            TO_DATE('1990-01-01', 'YYYY-MM-DD'),
            TO_DATE('9999-12-31', 'YYYY-MM-DD'),
            1,                                   -- active
            sysdate);
        COMMIT;
        
        log_writer('MSG', obj_name, sysdate, null, 'Merge(step1) into ce_employees_scd ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_employees_scd_merge_step1;
     
     -------------------------------------------------------------------------------------------------------------------        
    PROCEDURE etl_ce_employees_scd_insert_step2 AS
        obj_name VARCHAR2(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1)); 
    BEGIN      
        log_writer('MSG', obj_name, sysdate, null, 'Start insert(step2) into ce_employees_scd');
       INSERT INTO ce_employees_scd(employee_id,           
                            employee_src_id, 
                            employee_source_system,
                            employee_source_table,   
                            employee_first_name,          
                            employee_last_name,     
                            employee_email,          
                            employee_phone,
                            start_dt,
                            end_dt,
                            is_active,
                            insert_dt)
SELECT  (SELECT employee_id FROM ce_employees_scd WHERE employee_src_id = to_char(wrk.employee_src_id)
                                                         AND TRUNC(end_dt) = TRUNC(sysdate)),              
        wrk.employee_src_id,
        'SA_INSTORE',
        'SA_EMPLOYEES',
        wrk.employee_first_name ,
        wrk.employee_last_name ,
        wrk.employee_email ,
         wrk.employee_phone,
        sysdate,
        TO_DATE('9999-12-31', 'YYYY-MM-DD'),
        1,                                   -- active 
        sysdate
       
FROM    (SELECT employee_src_id ,
                employee_first_name ,
                employee_last_name ,
                employee_email ,
                employee_phone 
        FROM wrk_ce_employees_scd
        WHERE employee_src_id IN (  
                                        SELECT employee_src_id 
                                        FROM ce_employees_scd
                                        WHERE TRUNC(end_dt) = TRUNC(sysdate)
                                    )
        ) wrk;  
                                                  
        
        COMMIT;
        
        log_writer('MSG', obj_name, sysdate, null, 'Insert(step2) into ce_employees_scd ' || SQL%ROWCOUNT || ' rows.');
        
        EXCEPTION
            WHEN OTHERS THEN 
                log_writer('ERR', obj_name, sysdate, SQLCODE, SQLERRM);
                
     END etl_ce_employees_scd_insert_step2;
     
     -------------------------------------------------------------------------------------------------------------------     
     
END pkg_etl_ce_employees_scd;

-- TRY
exec pkg_etl_ce_employees_scd.etl_wrk_ce_employees_scd;
exec pkg_etl_ce_employees_scd.etl_ce_employees_scd_merge_step1;
exec pkg_etl_ce_employees_scd.etl_ce_employees_scd_insert_step2;
