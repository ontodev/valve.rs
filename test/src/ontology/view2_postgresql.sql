create table if not exists view2_table (foo text, bar text);
drop view if exists view2;
create view view2 as select * from view2_table;
insert into view2_table values ('d;e', 'e'), ('e', 'f'), ('d', 'c'), ('e', 'z'), ('e', 'w');
