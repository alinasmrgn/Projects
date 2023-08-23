drop table sa_instore.sa_channels;
create table sa_instore.sa_channels
as
(select * from sa_instore.ext_channels);

alter table sa_instore.sa_channels
add constraint sa_channels_pk primary key (channel_surr_id);
select * from sa_instore.sa_channels;
------------------------------------------------------------------------------------------------