create table if not exists view3_table (foo text, bar text);
create view if not exists view3 as select * from view3_table;
insert into view3_table values ('d;e', 'e'), ('e', 'f'), ('d', 'c'), ('e', 'z'), ('e', 'w');
