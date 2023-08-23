drop table sa_instore.sa_employees;
create table sa_instore.sa_employees
as
(select * from sa_instore.ext_employees);

alter table sa_instore.sa_employees
add constraint sa_employees_pk primary key (employee_surr_id);
select * from sa_instore.sa_employees;