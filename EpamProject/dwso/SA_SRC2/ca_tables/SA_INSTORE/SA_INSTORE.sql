drop table sa_instore.sa_channels;
create table sa_instore.sa_channels
as
(select * from sa_instore.ext_channels);

alter table sa_instore.sa_channels
add constraint sa_channels_pk primary key (channel_surr_id);
select * from sa_instore.sa_channels;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_sales_instore;
create table sa_instore.sa_sales_instore
as
(select * from sa_instore.ext_sales_instore);


select * from sa_instore.sa_sales_instore;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_customer_instore;
create table sa_instore.sa_customer_instore
as
(select * from sa_instore.ext_customer_instore);

alter table sa_instore.sa_customer_instore
add constraint sa_customer_instore_pk primary key (customer_surr_id);
select * from sa_instore.sa_customer_instore;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_payment_types;
create table sa_instore.sa_payment_types
as
(select * from sa_instore.ext_payment_types);

alter table sa_instore.sa_payment_types
add constraint sa_payment_types_pk primary key (payment_type_surr_id);
select * from sa_instore.sa_payment_types;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_products;
create table sa_instore.sa_products
as
(select * from sa_instore.ext_products);

alter table sa_instore.sa_products
add constraint sa_products_pk primary key (product_surr_id);
select * from sa_instore.sa_products;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_delivery_address;
create table sa_instore.sa_delivery_address
as
(select * from sa_instore.ext_delivery_address);

alter table sa_instore.sa_delivery_address
add constraint sa_delivery_address_pk primary key (delivery_address_surr_id);
select * from sa_instore.sa_delivery_address;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_employees;
create table sa_instore.sa_employees
as
(select * from sa_instore.ext_employees);

alter table sa_instore.sa_employees
add constraint sa_employees_pk primary key (employee_surr_id);
select * from sa_instore.sa_employees;
------------------------------------------------------------------------------------------------





























































