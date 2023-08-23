CREATE OR REPLACE PACKAGE pkg_etl_ce_employees_scd
AS  
    PROCEDURE etl_wrk_ce_employees_scd;
    PROCEDURE etl_ce_employees_scd_merge_step1;
    PROCEDURE etl_ce_employees_scd_insert_step2;

END pkg_etl_ce_employees_scd; 