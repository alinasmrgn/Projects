CREATE OR REPLACE PACKAGE pkg_etl_load
AS
    PROCEDURE etl_init_load;
    PROCEDURE etl_reload(start_reload_dt VARCHAR2, end_reload_dt VARCHAR2);
END pkg_etl_load;
