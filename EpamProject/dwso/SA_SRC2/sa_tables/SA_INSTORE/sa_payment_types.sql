drop table sa_instore.sa_payment_types;
create table sa_instore.sa_payment_types
as
(select * from sa_instore.ext_payment_types);

alter table sa_instore.sa_payment_types
add constraint sa_payment_types_pk primary key (payment_type_surr_id);
select * from sa_instore.sa_payment_types;