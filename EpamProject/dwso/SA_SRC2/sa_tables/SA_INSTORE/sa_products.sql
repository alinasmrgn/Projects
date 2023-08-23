drop table sa_instore.sa_products;
create table sa_instore.sa_products
as
(select * from sa_instore.ext_products);

alter table sa_instore.sa_products
add constraint sa_products_pk primary key (product_surr_id);
select * from sa_instore.sa_products;