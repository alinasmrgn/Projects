drop table sa_instore.sa_customer_instore;
create table sa_instore.sa_customer_instore
as
(select * from sa_instore.ext_customers_instore);

alter table sa_instore.sa_customer_instore
add constraint sa_customer_instore_pk primary key (customer_surr_id);
select * from sa_instore.sa_customer_instore;