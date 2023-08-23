drop table ext_channels;
Create table ext_channels(
    channel_surr_id number,
    channel_name number,
    channel_class varchar2(255),
    channel_class_id number
)

ORGANIZATION EXTERNAL(
    TYPE oracle_loader
    DEFAULT DIRECTORY bicycle_data
    ACCESS PARAMETERS 
    (RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL)
  LOCATION (bicycle_data:'ext_channels.csv')
)
PARALLEL 5
REJECT LIMIT UNLIMITED;

Select * from ext_channels;
