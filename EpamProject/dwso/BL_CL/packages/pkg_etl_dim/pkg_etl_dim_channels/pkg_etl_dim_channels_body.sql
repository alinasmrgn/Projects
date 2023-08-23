CREATE OR REPLACE PACKAGE BODY pkg_etl_dim_channels
AS
    PROCEDURE etl_dim_channels AS
        obj_name VARCHAR(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
      
    BEGIN
        log_writer('MSG', obj_name, sysdate, NULL, 'Start to merge into dim_channels.');
        
          Merge into dim_channels dm
            Using (Select   'BL_3NF' as scr_system,
                            'CE_CHANNELS' as scr_table,
                            c.channel_id  as scr_id,
                            c.channel_desc,
                            nvl(cc.channels_class_id,-99) as channel_class_id,
                            cc.channels_class as channel_class                           
                    From ce_channels c
                    Left join ce_channels_classes cc on c.channel_class_id = cc.channels_class_id
                    Where c.channel_id > = 0) ce
        ON (dm.source_id = to_char(ce.scr_id))
        when matched then 
            Update set  dm.channel_name = ce.channel_desc,
                        dm.channel_class_id = ce.channel_class_id,
                        dm.channel_class = ce.channel_class,
                        update_dt = sysdate
            Where   decode(dm.channel_name, ce.channel_desc, 0, 1) + 
                    decode( dm.channel_class_id, ce.channel_class_id, 0, 1) + 
                    decode( dm.channel_class, ce.channel_class, 0, 1)   > 0
         When not matched then 
            Insert (channel_surr_id,
                    source_system,
                    source_table,
                    source_id,
                    channel_name,
                    channel_class,
                    channel_class_id,
                    insert_dt,
                    update_dt)
                Values (DIM_CHANNELS_S.nextval,
                        ce.scr_system,
                        ce.scr_table,
                        ce.scr_id,
                        ce.channel_desc,
                        ce.channel_class,
                        ce.channel_class_id,
                        sysdate,
                        sysdate);    
                    
        log_writer('MSG', obj_name, sysdate, NULL, 'Merged into dim_channels '|| SQL%ROWCOUNT || ' rows.' ); 

        COMMIT;        
        
        EXCEPTION
            WHEN OTHERS THEN
                log_writer('ERR', obj_name, sysdate, SQLCODE, sqlerrm);
    END etl_dim_channels;      

END pkg_etl_dim_channels;

-- TRY
exec pkg_etl_dim_channels.etl_dim_channels;