Merge into dim_payment_types dm
            Using (Select   'BL_3NF' as scr_system,
                            'CE_PAYMENT_TYPES' as scr_table,
                            p.payment_type_id  as scr_id,
                            p.payment_type_name                           
                    From ce_payment_types p
                    Where p.payment_type_id > = 0) ce
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
commit;
select * from dim_payment_types;