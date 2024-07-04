create table if not exists view1_table (foo text, bar text);
create view if not exists view1 as select * from view1_table;
insert into view1_table values ('d;e', 'e'), ('e', 'f'), ('d', 'c'), ('e', 'z'), ('e', 'w');
