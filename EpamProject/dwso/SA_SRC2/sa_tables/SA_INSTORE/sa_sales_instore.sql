drop table sa_instore.sa_sales_instore;
drop table sa_instore.sa_sales_instore1;
drop table sa_instore.sa_sales_instore2;
drop table sa_instore.sa_sales_instore3;

CREATE TABLE sa_instore.sa_sales_instore3(
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
  quantity NUMBER,
  update_dt date
);
insert into sa_sales_instore3(transaction_id,customer_surr_id, delivery_address_surr_id, employee_surr_id, product_surr_id, channel_surr_id, payment_type_surr_id, date_id, revenue, unit_cost, unit_price, quantity,update_dt)
 select   si.transaction_id, si.customer_surr_id, si.delivery_address_surr_id, si.employee_surr_id, si.product_surr_id, si.channel_surr_id, si.payment_type_surr_id, si.date_id, 
 (si.unit_price - si.unit_cost) as revenue, si.unit_cost, si.unit_price, 1, sysdate
 from sa_sales_instore2 si
 ; 
 commit;

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