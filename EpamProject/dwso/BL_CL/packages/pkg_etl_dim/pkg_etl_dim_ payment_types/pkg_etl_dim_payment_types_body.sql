CREATE OR REPLACE PACKAGE BODY pkg_etl_dim_payment_types
AS
    PROCEDURE etl_dim_payment_types AS
        obj_name VARCHAR(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    
    TYPE for_dim_payment_types IS RECORD (payment_type_id ce_payment_types.payment_type_id%TYPE, 
                                        payment_type_name ce_payment_types.payment_type_name%TYPE);       -- take from ce_payment_types neseccary columns and create a new type
         
        TYPE payment_type IS TABLE OF for_dim_payment_types                                       -- create a new type of Associative array
                        INDEX BY PLS_INTEGER ;                       
        t_payment_type payment_type;
        TYPE ref_cur IS REF CURSOR;             -- type of ref cursor
        cur ref_cur;                            -- variable of ref cusrsor

    BEGIN
        log_writer('MSG', obj_name, sysdate, NULL, 'Start to merge into dim_payment_types.');
       
       OPEN cur  FOR   'SELECT payment_type_id, payment_type_name                     
                        FROM ce_payment_types
                        Where payment_type_id > 0';
        LOOP
            EXIT WHEN cur%notfound;
            
            FETCH cur
            BULK COLLECT INTO t_payment_type;                           -- put into t_payment_type not only one row but a collection of rows  
        
            FORALL idx IN t_payment_type.FIRST..t_payment_type.LAST         -- send merge to sql engine not only for a row but portions       
       
       Merge into dim_payment_types dm
            Using (Select   'BL_3NF' as scr_system,
                            'CE_PAYMENT_TYPES' as scr_table,
                            t_payment_type(idx).payment_type_id  as scr_id,
                            t_payment_type(idx).payment_type_name as payment_type_name                           
                    From dual) ce
        ON (dm.source_id = to_char(ce.scr_id))
        when matched then 
            Update set  dm.payment_type_name = ce.payment_type_name,
                        update_dt = sysdate
            Where   decode(dm.payment_type_name, ce.payment_type_name, 0, 1) > 0
         When not matched then 
            Insert (payment_type_surr_id,
                    source_system,
                    source_table,
                    source_id,
                    payment_type_name,
                    insert_dt,
                    update_dt)
                Values (DIM_PAYMENT_TYPES_S.nextval,
                        ce.scr_system,
                        ce.scr_table,
                        ce.scr_id,
                        ce.payment_type_name,
                        sysdate,
                        sysdate);
          End loop;   
          
        log_writer('MSG', obj_name, sysdate, NULL, 'Merged into dim_payment_types '|| SQL%ROWCOUNT || ' rows.' );            
         Close cur;
        COMMIT;
        
        EXCEPTION
            WHEN OTHERS THEN
                log_writer('ERR', obj_name, sysdate, SQLCODE, sqlerrm);
    END etl_dim_payment_types;      

END pkg_etl_dim_payment_types;

-- TRY
exec pkg_etl_dim_payment_types.etl_dim_payment_types;


