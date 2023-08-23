drop  table wrk_ce_employees_scd;
Create table wrk_ce_employees_scd(
    employee_src_id         varchar(20) not null,
    employee_first_name          VARCHAR2(50) NOT NULL,
    employee_last_name          VARCHAR2(50) NOT NULL,
    employee_email          VARCHAR2(100) NOT NULL,
    employee_phone          VARCHAR2(10) NOT NULL
);
select * from wrk_ce_employees_scd;
delete from wrk_ce_employees_scd;