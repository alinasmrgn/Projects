drop table sa_online.sa_sales_online;
drop table sa_online.sa_sales_online1;
drop table sa_online.sa_sales_online2;
CREATE TABLE sa_online.sa_sales_online2(
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
insert into sa_sales_online2(transaction_id,customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id, revenue, unit_cost, unit_price, quantity)
 select   CE_SALES1_S.nextval, si.customer_surr_id, si.delivery_address_surr_id, si.employee_surr_id, si.product_surr_id, si.channel_surr_id, si.payment_type_surr_id, si.date_id,
 (si.unit_price - si.unit_cost) as revenue, si.unit_cost, si.unit_price, 1
 from sa_sales_online si
 ; 
 commit;
DROP sequence CE_SALES2_S; 
create sequence CE_SALES2_S
    increment by 1
    start with 0
    nomaxvalue
    minvalue 0
    nocycle
    nocache;

CREATE TABLE sa_online.sa_sales_online1(
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

CREATE TABLE sa_online.sa_sales_online(
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

select * from sa_online.sa_sales_online2 order by transaction_id;
commit;
insert into sa_sales_online(customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id, revenue, unit_cost, unit_price)
 select distinct si.customer_surr_id, si.delivery_address_surr_id, si.employee_surr_id, si.product_surr_id, si.channel_surr_id, si.payment_type_surr_id, si.date_id,
 (esi.unit_price - esi.unit_cost) as revenue, esi.unit_cost, esi.unit_price
 from sa_sales_online1 si
 inner join sa_instore.ext_DIM_PRODUCTS esi 
 on  si.product_surr_id = esi.product_surr_id ;
 
declare
    i number:=0;
  begin loop
    i:=i+1;
     INSERT INTO sa_sales_online1 (customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id)
values((select customer_surr_id from sa_customer_online order by dbms_random.value fetch first 1 row only),
            round(DBMS_RANDOM.VALUE(0,3491)) ,
            round(DBMS_RANDOM.VALUE(0,5)) ,
            round(DBMS_RANDOM.VALUE(0,49)) ,
            round(DBMS_RANDOM.VALUE(0,4)) ,
            round(DBMS_RANDOM.VALUE(0,5)),
         (select to_date('2019-01-01', 'yyyy-mm-dd')+trunc(dbms_random.value(1,1000)) from dual)
         );
    exit when (i>=150000);
  end loop;
end;
commit;

select count(*) from sa_sales_online1;
select * from sa_sales_online order by unit_price desc;


select * from sa_online.sa_sales_online ORDER BY DATE_ID DESC;
------------------------------------------------------------------------------------------------
drop table sa_online.sa_customer_online;
create table sa_online.sa_customer_online
as
(select * from sa_online.ext_customers_online);

alter table sa_online.sa_customer_online
add constraint sa_customer_online_pk primary key (customer_surr_id);
select * from sa_online.sa_customer_online;
------------------------------------------------------------------------------------------------