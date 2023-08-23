drop PACKAGE pkg_etl_fct_sales ;
CREATE OR REPLACE PACKAGE pkg_etl_fct_sales IS
    PROCEDURE ld_wrk_sales;
    PROCEDURE etl_ce_sales;
    PROCEDURE etl_fct_sales(start_dt varchar2,end_dt varchar2);

END pkg_etl_fct_sales;