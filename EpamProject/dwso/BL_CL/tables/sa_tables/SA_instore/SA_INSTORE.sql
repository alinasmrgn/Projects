drop table sa_instore.sa_channels;
create table sa_instore.sa_channels
as
(select * from sa_instore.ext_channels);

alter table sa_instore.sa_channels
add constraint sa_channels_pk primary key (channel_surr_id);
select * from sa_instore.sa_channels;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_sales_instore;
drop table sa_instore.sa_sales_instore1;
drop table sa_instore.sa_sales_instore2;
CREATE TABLE sa_instore.sa_sales_instore2(
    transaction_id NUMBER,
    customer_surr_id      NUMBER,
  delivery_address_surr_id NUMBER,
  employee_surr_id     NUMBER,
  product_surr_id   NUMBER,
  channel_surr_id      NUMBER,
  payment_type_surr_id     NUMBER,
  date_id date,
  revenue number,
    unit_price NUMBER ,
  unit_cost NUMBER,
  quantity NUMBER
);
insert into sa_sales_instore2(transaction_id,customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id, revenue, unit_cost, unit_price, quantity)
 select   CE_SALES1_S.nextval, si.customer_surr_id, si.delivery_address_surr_id, si.employee_surr_id, si.product_surr_id, si.channel_surr_id, si.payment_type_surr_id, si.date_id,
 (si.unit_price - si.unit_cost) as revenue, si.unit_cost, si.unit_price, 1
 from sa_sales_instore si
 ; 
 CREATE PUBLIC SYNONYM CE_SALES1_S FOR sa_instore.CE_SALES1_S;
 commit;
DROP sequence CE_SALES1_S; 
create sequence CE_SALES1_S
    increment by 1
    start with 0
    nomaxvalue
    minvalue 0
    nocycle
    nocache;
    
CREATE TABLE sa_instore.sa_sales_instore1(
    customer_surr_id      NUMBER,
  delivery_address_surr_id NUMBER,
  employee_surr_id     NUMBER,
  product_surr_id   NUMBER,
  channel_surr_id      NUMBER,
  payment_type_surr_id     NUMBER,
  date_id date,
  revenue number,
    unit_price NUMBER ,
  unit_cost NUMBER
);

CREATE TABLE sa_instore.sa_sales_instore(
    customer_surr_id      NUMBER,
  delivery_address_surr_id NUMBER,
  employee_surr_id     NUMBER,
  product_surr_id   NUMBER,
  channel_surr_id      NUMBER,
  payment_type_surr_id     NUMBER,
  date_id date,
  revenue number,
    unit_price NUMBER ,
  unit_cost NUMBER
);

select * from sa_instore.sa_sales_instore order by customer_surr_id , date_id;

insert into sa_sales_instore(customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id, revenue, unit_cost, unit_price)
 select distinct si.customer_surr_id, si.delivery_address_surr_id, si.employee_surr_id, si.product_surr_id, si.channel_surr_id, si.payment_type_surr_id, si.date_id,
 (esi.unit_price - esi.unit_cost) as revenue, esi.unit_cost, esi.unit_price
 from sa_sales_instore1 si
 inner join ext_DIM_PRODUCTS esi 
 on  si.product_surr_id = esi.product_surr_id 
 ; 
 commit;
 
declare
    i number:=0;
  begin loop
    i:=i+1;
     INSERT INTO sa_sales_instore1 (customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id)
values((select customer_surr_id from sa_customer_instore order by dbms_random.value fetch first 1 row only),
            round(DBMS_RANDOM.VALUE(0,3491)) ,
            round(DBMS_RANDOM.VALUE(0,5)) ,
            round(DBMS_RANDOM.VALUE(0,49)) ,
            round(DBMS_RANDOM.VALUE(0,4)) ,
            round(DBMS_RANDOM.VALUE(0,5)),
         (select to_date('2019-01-01', 'yyyy-mm-dd')+trunc(dbms_random.value(1,1000)) from dual)
         );
    exit when (i>=600000);
  end loop;
end;

select count(*) from sa_sales_instore;
select * from sa_sales_instore order by unit_price desc;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_customer_instore;
create table sa_instore.sa_customer_instore
as
(select * from sa_instore.ext_customers_instore);

alter table sa_instore.sa_customer_instore
add constraint sa_customer_instore_pk primary key (customer_surr_id);
select * from sa_instore.sa_customer_instore;
------------------------------------------------------------------------------------------------
drop table sa_instore.sa_customer_job_categories;
create table sa_instore.sa_customer_job_categories
as
(select * from sa_instore.ext_customers_job_categories);

alter table sa_instore.sa_customer_job_categories
add constraint sa_customer_job_categories_pk primary key (customer_job_category_id);
select * from sa_instore.sa_customer_job_categories;

------------------------------------------------------------------------------------------------
drop table sa_instore.sa_customer_jobs;
create table sa_instore.sa_customer_jobs
as
(select * from sa_instore.ext_customers_jobs);

alter table sa_instore.sa_customer_jobs
add constraint sa_customer_jobs_pk primary key (customer_job_id);
select * from sa_instore.sa_customer_jobs;
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





























































