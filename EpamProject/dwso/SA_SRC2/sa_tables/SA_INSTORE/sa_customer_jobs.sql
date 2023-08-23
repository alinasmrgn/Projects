drop table sa_instore.sa_customer_jobs;
create table sa_instore.sa_customer_jobs
as
(select * from sa_instore.ext_customers_jobs);

alter table sa_instore.sa_customer_jobs
add constraint sa_customer_jobs_pk primary key (customer_job_id);
select * from sa_instore.sa_customer_jobs;