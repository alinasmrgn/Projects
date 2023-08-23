CREATE OR REPLACE DIRECTORY bicycle_data AS '/opt/oracle/oradata/bicycle';

 -- folder in docker/oracle-xe
grant read, write on directory bicycle_data to SA_ONLINE;
grant read, write on directory bicycle_data to SA_INSTORE;
