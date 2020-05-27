/*test covers the following use cases:
- a record with one update ('123')
- a record with more than one update ('456')
- a record with a delete ('789')
- an insert record accidentally sent for a column that was already inserted in a prior batch ('101112')
- column list is mismatched (dml_indicator only exists in source)
- datatype mismatch (mismatch_col)
- column order mismatch (txt_value and version)
*/

/*creates the two test tables*/
drop table if exists tgt_test;

create table tgt_test
    (
    id   varchar(50),
    version   varchar(10),
    txt_value   varchar(50),
    mismatch_col  int
    )
;

drop table if exists src_test;

create table src_test
    (
    id   varchar(50),
    txt_value   varchar(50),
    version   varchar(10),
    dml_indicator varchar(10),
    mismatch_col  varchar(50)
    )
;


/*inserts values into the target table.
This would be how the data looked on first insert, prior to updates coming in*/
insert into tgt_test values ('123', '1', 'original insert', -99);
insert into tgt_test values ('456', '1', 'original insert', -98);
insert into tgt_test values ('789', '1', 'original insert', -97);
insert into tgt_test values ('101112', '1', 'original insert', -96);


/*this includes a delete, a multi-update, 
and an insert where the version is incorrect (it reused the original version number)*/
insert into src_test values ('123', '1st update', '2', 'U', '-99');
insert into src_test values ('456', '1st update', '2', 'U', '-98');
insert into src_test values ('456', '2nd update', '3', 'U', '-97');
insert into src_test values ('789', 'delete', '2', 'D', '-96');
insert into src_test values ('101112', 'accidental second insert', '1', 'I', '-95');


call upsert_sp('tgt_test', 'id', 'version', 'src_test', 'id', 'version', 'dml_indicator');

--select * FROM src_test;
select * FROM tgt_test;

/*expected results:
id        version     txt_value                 mismatch_col
456       3           2nd update                NULL
101112    1           accidental second insert  NULL
123       2           1st update                NULL
*/

--Drop  PROCEDURE upsert_sp(target_table INOUT character varying, target_key INOUT character varying, target_version INOUT character varying, source_table INOUT character varying, source_key INOUT character varying, source_version INOUT character varying, source_dml_indicator INOUT character varying)
