drop table sa_instore.sa_customer_job_categories;
create table sa_instore.sa_customer_job_categories
as
(select * from sa_instore.ext_customers_job_categories);

alter table sa_instore.sa_customer_job_categories
add constraint sa_customer_job_categories_pk primary key (customer_job_category_id);
select * from sa_instore.sa_customer_job_categories;