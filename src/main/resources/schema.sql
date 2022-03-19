drop table if exists  customer ;
create table   customer
(
    id   serial primary key,
    name varchar(255) not null
);