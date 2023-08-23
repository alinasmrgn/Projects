CREATE OR REPLACE PACKAGE BODY pkg_etl_dim_products
AS
    PROCEDURE etl_dim_products AS
        obj_name VARCHAR(200) := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(1));
    BEGIN
        log_writer('MSG', obj_name, sysdate, NULL, 'Start to merge into dim_products.');
        Merge into dim_products dm
            Using (Select   'BL_3NF' as scr_system,
                            'CE_PRODUCTS' as scr_table,
                            p.product_id  as scr_id,
                            p.product_line,
                            p.product_size,
                            p.product_class,
                            nvl(b.product_brand_id,-99) as product_brand_id, b.product_brand                           
                    From ce_products p
                    Left join ce_product_brands b on p.product_brand_id = b.product_brand_id
                    Where p.product_id > = 0) ce
        ON (dm.source_id = ce.scr_id)
        when matched then 
            Update set  dm.product_line = ce.product_line,
                        dm.product_size = ce.product_size,
                        dm.product_class = ce.product_class,
                        dm.product_brand_id = ce.product_brand_id,
                        dm.product_brand = ce.product_brand,
                        update_dt = sysdate
            Where   decode(dm.product_line, ce.product_line, 0, 1) + 
                    decode( dm.product_size, ce.product_size, 0, 1) + 
                    decode( dm.product_class, ce.product_class, 0, 1) + 
                    decode(dm.product_brand_id, ce.product_brand_id, 0, 1) + 
                    decode(dm.product_brand, ce.product_brand, 0, 1)  > 0
         When not matched then 
            Insert (product_surr_id,
                    source_system,
                    source_table,
                    source_id,
                    product_brand,
                    product_brand_id,
                    product_line,
                    product_size,
                    product_class,
                    insert_dt,
                    update_dt)
                Values (DIM_PRODUCTS_S.nextval,
                        ce.scr_system,
                        ce.scr_table,
                        ce.scr_id,
                        ce.product_brand,
                        ce.product_brand_id,
                        ce.product_line,
                        ce.product_size,
                        ce.product_class,
                        sysdate,
                        sysdate);
                        
        log_writer('MSG', obj_name, sysdate, NULL, 'Merged into dim_products '|| SQL%ROWCOUNT || ' rows.' );            
        COMMIT;
        
        
        EXCEPTION
            WHEN OTHERS THEN
                log_writer('ERR', obj_name, sysdate, SQLCODE, sqlerrm);
    END etl_dim_products;      

END pkg_etl_dim_products;

-- TRY
exec pkg_etl_dim_products.etl_dim_products;




