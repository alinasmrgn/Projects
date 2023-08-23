CREATE OR REPLACE PACKAGE BODY pkg_etl_dim_employees_scd 
AS
    PROCEDURE etl_dim_employees_scd AS
        obj_name VARCHAR(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
        log_writer('MSG', obj_name, sysdate, NULL, 'Start to merge into dim_employees_scd .');
        Merge into dim_employees_scd dm
            Using ( 
                    Select  'BL_3NF' as scr_system,
                            'CE_EMPLOYEES_SCD' as scr_table,
                            emp.EMPLOYEE_ID as scr_id,
                            emp.EMPLOYEE_FIRST_NAME,
                            emp.EMPLOYEE_LAST_NAME,
                            emp.EMPLOYEE_EMAIL, 
                            emp.EMPLOYEE_PHONE, 
                            emp.START_DT,
                            emp.END_DT,
                            emp.IS_ACTIVE
                    From ce_employees_scd emp
                    Where emp.EMPLOYEE_ID > 0
                  ) ce
        On (dm.EMPLOYEE_SURR_ID = to_char(ce.scr_id) and dm.START_DT = ce.START_DT)
        When matched then 
            Update set  
                        dm.END_DT = ce.END_DT,
                        dm.IS_ACTIVE = ce.IS_ACTIVE
            Where   decode(dm.END_DT, ce.END_DT, 0, 1) > 0
        When not matched then 
            Insert (EMPLOYEE_SURR_ID,
                    source_system,
                    source_table,
                    source_id,
                    EMPLOYEE_FIRST_NAME,
                    EMPLOYEE_LAST_NAME,
                    EMPLOYEE_EMAIL,
                    EMPLOYEE_PHONE,
                    START_DT,
                    END_DT,
                    IS_ACTIVE,
                    insert_dt)
            Values (DIM_CUSTOMERS_S.nextval,
                    ce.scr_system,
                    ce.scr_table,
                    ce.scr_id,
                    ce.EMPLOYEE_FIRST_NAME, 
                    ce.EMPLOYEE_LAST_NAME, 
                    ce.EMPLOYEE_EMAIL, 
                    ce.EMPLOYEE_PHONE,
                    ce.START_DT, 
                    ce.END_DT, 
                    ce.IS_ACTIVE,
                    sysdate);     
                    
        log_writer('MSG', obj_name, sysdate, NULL, 'Merged into dim_employees_scd '|| SQL%ROWCOUNT || ' rows.' );            
        COMMIT;       
        
        EXCEPTION
            WHEN OTHERS THEN
                log_writer('ERR', obj_name, sysdate, SQLCODE, sqlerrm);
    END etl_dim_employees_scd;  
     

END pkg_etl_dim_employees_scd ;

-- TRY
exec pkg_etl_dim_employees_scd .etl_dim_employees_scd ;




