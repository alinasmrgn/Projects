CREATE DATABASE google_apps;

-- creating schema
drop schema if exists google_apps cascade;
create schema google_apps;

-- use schema
SET schema 'google_apps';


/*********************************
        creating tables
*********************************/
  
drop table if exists apps cascade;

CREATE SEQUENCE app_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

 CREATE TABLE apps
(
    app_id          integer DEFAULT nextval('app_id_seq'::regclass) NOT NULL,
    app_name        varchar(300),
    cat_id          int4,
    rating          float8,
    rating_count    int8,
    size_of_app     varchar(50),
    content_rating  varchar(50),
    check (content_rating  in ('Everyone 10+','Mature 17+','Everyone','Teen','D')),
    CONSTRAINT app_category_foreign FOREIGN KEY (cat_id) REFERENCES category (cat_id),
    CONSTRAINT apps_pkey PRIMARY KEY (app_id)
);
SELECT * FROM apps;


drop table if exists category cascade;

CREATE SEQUENCE cat_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

 CREATE TABLE category
(
    cat_id        integer DEFAULT nextval('cat_id_seq'::regclass) NOT NULL,
    cat_name      varchar(300),
    CONSTRAINT cat_pkey PRIMARY KEY (cat_id)
);

--SELECT * FROM category;


drop table if exists developer cascade;

CREATE SEQUENCE dev_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

 CREATE TABLE developer
(
    dev_id          integer DEFAULT nextval('dev_id_seq'::regclass) NOT NULL,
    dev_name        varchar(300),
    dev_website     varchar(300),
    dev_email       varchar(300),
    CONSTRAINT dev_pkey PRIMARY KEY (dev_id)
);
--SELECT * FROM google_app ;

drop table if exists installs cascade;



 CREATE TABLE installs
(
    app_id          int4 NOT NULL,
    dev_id          int4 NOT NULL,
    release_date    date,
    last_update     date,
    installs        int8,
    CONSTRAINT app_install_foreign FOREIGN KEY (app_id) REFERENCES apps (app_id),
    CONSTRAINT dev_install_foreign FOREIGN KEY (dev_id) REFERENCES developer (dev_id)
);
SELECT * FROM installs ;



insert into apps (app_name,cat_id,rating,rating_count,size_of_app,content_rating)
SELECT ga."App Name",c.cat_id, ga.rating, ga."Rating Count" , ga."Size", ga."Content Rating" 
from google_app ga
INNER JOIN category c ON ga.category=c.cat_name;


insert into developer (dev_name,dev_website,dev_email)
SELECT distinct "Developer Id","Developer Website", "Developer Email" 
from google_app 
WHERE "Developer Id" IS NOT NULL ;

insert into category (cat_name)
SELECT distinct category 
from google_app
WHERE category IS NOT NULL ;

insert into installs (app_id,dev_id, release_date, last_update, installs) 
select a.app_id, 
       d.dev_id,     
       ga.released,
       ga."Last Updated",
       ga."Maximum Installs"
     
from google_app ga join apps a
on ga."App Name"=a.app_name AND ga."Size"=a.size_of_app
                  join developer d    
on ga."Developer Id" = d.dev_name AND ga."Developer Website"=d.dev_website AND ga."Developer Email" =d.dev_email;

