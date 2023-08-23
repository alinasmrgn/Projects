drop table sa_instore.sa_delivery_address;
create table sa_instore.sa_delivery_address
as
(select * from sa_instore.ext_delivery_address);

alter table sa_instore.sa_delivery_address
add constraint sa_delivery_address_pk primary key (delivery_address_surr_id);
select * from sa_instore.sa_delivery_address;