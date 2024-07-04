create table if not exists view2_table (foo text, bar text);
create view if not exists view2 as select * from view2_table;
insert into view2_table values ('d;e', 'e'), ('e', 'f'), ('d', 'c'), ('e', 'z'), ('e', 'w');
