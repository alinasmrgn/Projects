ALTER SESSION SET "_ORACLE_SCRIPT" = true;

CREATE USER BL_CL IDENTIFIED BY BL_CL		
 DEFAULT TABLESPACE users
 TEMPORARY TABLESPACE temp
 PROFILE default;

GRANT connect, resource TO BL_CL;
GRANT UNLIMITED TABLESPACE TO BL_CL;
