/*creates the two test tables*/
drop table if exists tgt_test;
create table tgt_test
(id   varchar(50),
txt_value   varchar(50),
version   varchar(10),
dml_indicator varchar(10)
);

drop table if exists src_test;
create table src_test
(id   varchar(50),
txt_value   varchar(50),
version   varchar(10),
dml_indicator varchar(10)
);


/*inserts values into the target table.
This would be how the data looked on first insert,
prior to updates coming in*/
insert into tgt_test values ('123', 'original insert', '1', 'I');
insert into tgt_test values ('456', 'original insert', '1', 'I');
insert into tgt_test values ('789', 'original insert', '1', 'I');
insert into tgt_test values ('101112', 'original insert', '1', 'I');


/*this includes a delete, a multi-update, 
and an insert where the version is incorrect
(it reused the original version number)*/
insert into src_test values ('123', '1st update', '2', 'U');
insert into src_test values ('456', '1st update', '2', 'U');
insert into src_test values ('456', '2st update', '3', 'U');
insert into src_test values ('789', 'delete', '2', 'D');
insert into src_test values ('101112', 'accidental second insert', '1', 'I');


call upsert_sp('tgt_test', 'id', 'version', 'src_test', 'id', 'version', 'dml_indicator');

select * FROM tgt_test;
select * FROM src_test;





--Drop  PROCEDURE upsert_sp(target_table INOUT character varying, target_key INOUT character varying, target_version INOUT character varying, source_table INOUT character varying, source_key INOUT character varying, source_version INOUT character varying, source_dml_indicator INOUT character varying)
