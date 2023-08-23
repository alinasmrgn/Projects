DROP TABLE prm_mta_incremental_load;
CREATE TABLE prm_mta_incremental_load(
    sa_table_name VARCHAR2(100),
    target_table_name VARCHAR2(100),
    "package" VARCHAR2(100),
    "procedure" VARCHAR2(100),
    previous_load_date DATE     
);

CREATE OR REPLACE PUBLIC SYNONYM prm_mta_incremental_load FOR bl_cl.prm_mta_incremental_load;


Insert into prm_mta_incremental_load (sa_table_name, target_table_name, "package", "procedure", previous_load_date)
Values ('SA_SALES_INSTORE', 'CE_SALES', 'PKG_ETL_CE_SALES', 'ETL_CE_SALES', to_date('01.01.1900', 'dd.mm.yyyy'));

Insert into prm_mta_incremental_load (sa_table_name, target_table_name, "package", "procedure", previous_load_date)
Values ('SA_SALES_ONLINE', 'CE_SALES', 'PKG_ETL_CE_SALES', 'ETL_CE_SALES', to_date('01.01.1900', 'dd.mm.yyyy'));

Insert into prm_mta_incremental_load (sa_table_name, target_table_name, "package", "procedure", previous_load_date)
Values ('SA_EMPLOYYES', 'CE_EMPLOYEES_SCD', 'PKG_ETL_CE_EMPLOYEES_SCD', 'ETL_CE_EMPLOYEES_SCD',to_date('01.01.1900', 'dd.mm.yyyy'));

Insert into prm_mta_incremental_load (sa_table_name, target_table_name, "package", "procedure", previous_load_date)
Values ('wrk_sales', 'ce_sales', 'pkg_etl_ce_sales', 'etl_ce_sales',to_date('01.01.1900', 'dd.mm.yyyy'));

COMMIT;





