ALTER SESSION SET "_ORACLE_SCRIPT" = true;

CREATE USER BL_DM IDENTIFIED BY BL_DM 		
 DEFAULT TABLESPACE users
 TEMPORARY TABLESPACE temp
 PROFILE default;

GRANT connect, resource TO BL_DM ;
GRANT UNLIMITED TABLESPACE TO BL_DM;
