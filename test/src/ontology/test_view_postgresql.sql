create table if not exists test_table (foo text, bar text);
create or replace view test_view as select * from test_table;
insert into test_table values ('d;e', 'e'), ('e', 'f'), ('d', 'c'), ('e', 'z'), ('e', 'w');
