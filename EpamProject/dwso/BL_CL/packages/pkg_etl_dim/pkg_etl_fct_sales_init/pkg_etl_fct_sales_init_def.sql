CREATE OR REPLACE PACKAGE pkg_etl_fct_sales_init AS
    PROCEDURE ld_wrk_sales_init;
    PROCEDURE etl_ce_sales_init;
    PROCEDURE etl_fct_sales_init;
END pkg_etl_fct_sales_init;

