CREATE OR REPLACE PACKAGE BODY pkg_etl_dim_delivery_addresses
AS
    PROCEDURE etl_dim_delivery_addresses AS
        obj_name VARCHAR(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
        log_writer('MSG', obj_name, sysdate, NULL, 'Start to merge into dim_delivery_addresses.');
        Merge into dim_delivery_addresses dm
            Using (Select   'BL_3NF' as scr_system,
                            'CE_DELIVERY_ADDRESSES' as scr_table,
                            da.delivery_address_id  as scr_id,
                            da.DELIVERY_STREET_ADDRESS,
                            da.DELIVERY_POSTAL_CODE,
                            da.DELIVERY_COUNTRY,
                            nvl(ds.DELIVERY_STATE_ID,-99) as DELIVERY_STATE_ID, ds.DELIVERY_STATE                           
                    From ce_delivery_addresses da
                    Left join ce_delivery_states ds on da.DELIVERY_STATE_ID = ds.DELIVERY_STATE_ID
                    Where da.delivery_address_id > = 0) ce
        ON (dm.source_id = to_char(ce.scr_id))
        when matched then 
            Update set  dm.DELIVERY_STREET_ADDRESS = ce.DELIVERY_STREET_ADDRESS,
                        dm.DELIVERY_POSTAL_CODE = ce.DELIVERY_POSTAL_CODE,
                        dm.DELIVERY_COUNTRY = ce.DELIVERY_COUNTRY,
                        dm.DELIVERY_STATE_ID = ce.DELIVERY_STATE_ID,
                        dm.DELIVERY_STATE = ce.DELIVERY_STATE,
                        update_dt = sysdate
            Where   decode(dm.DELIVERY_STREET_ADDRESS, ce.DELIVERY_STREET_ADDRESS, 0, 1) + 
                    decode( dm.DELIVERY_POSTAL_CODE, ce.DELIVERY_POSTAL_CODE, 0, 1) + 
                    decode( dm.DELIVERY_COUNTRY, ce.DELIVERY_COUNTRY, 0, 1) + 
                    decode(dm.DELIVERY_STATE_ID, ce.DELIVERY_STATE_ID, 0, 1) + 
                    decode(dm.DELIVERY_STATE, ce.DELIVERY_STATE, 0, 1)  > 0
         When not matched then 
            Insert (delivery_address_surr_id,
                    source_system,
                    source_table,
                    source_id,
                    DELIVERY_STREET_ADDRESS,
                    DELIVERY_POSTAL_CODE,
                    DELIVERY_STATE_ID,
                    DELIVERY_STATE,
                    DELIVERY_COUNTRY,
                    insert_dt,
                    update_dt)
                Values (DIM_DELIVERY_ADDRESSES_S.nextval,
                        ce.scr_system,
                        ce.scr_table,
                        ce.scr_id,
                        ce.DELIVERY_STREET_ADDRESS,
                        ce.DELIVERY_POSTAL_CODE,
                        ce.DELIVERY_STATE_ID,
                        ce.DELIVERY_STATE,
                        ce.DELIVERY_COUNTRY,
                        sysdate,
                        sysdate);
                    
        log_writer('MSG', obj_name, sysdate, NULL, 'Merged into dim_delivery_addresses '|| SQL%ROWCOUNT || ' rows.' );              
        COMMIT;        
        
        EXCEPTION
            WHEN OTHERS THEN
                log_writer('ERR', obj_name, sysdate, SQLCODE, sqlerrm);
    END etl_dim_delivery_addresses;      

END pkg_etl_dim_delivery_addresses;

-- TRY
exec pkg_etl_dim_delivery_addresses.etl_dim_delivery_addresses;








