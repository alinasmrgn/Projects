drop table sa_online.sa_customer_online;
create table sa_online.sa_customer_online
as
(select * from sa_online.ext_customers_online);

alter table sa_online.sa_customer_online
add constraint sa_customer_online_pk primary key (customer_surr_id);
select * from sa_online.sa_customer_online;
------------------------------------------------------------------------------------------------