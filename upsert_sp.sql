CREATE OR REPLACE PROCEDURE sp_upsert_tester()
AS $$
DECLARE 
    cursor_var int;

BEGIN



/*creates the two test tables*/
drop table if exists tgt_test_short;

create table tgt_test_short
    (
    id   varchar(50),
    version   varchar(10),
    txt_value   varchar(50),
    mismatch_col  int
    )
    sortkey(id)
;

drop table if exists src_test_short;

create table src_test_short
    (
    id   varchar(50),
    txt_value   varchar(50),
    version   varchar(10),
    dml_indicator varchar(10),
    mismatch_col  varchar(50)
    )
    SORTKEY(id)
;


/*inserts values into the target table.
This would be how the data looked on first insert, prior to updates coming in*/
insert into tgt_test_short values ('123', '1', 'original insert', -99);
insert into tgt_test_short values ('456', '1', 'original insert', -98);
insert into tgt_test_short values ('789', '1', 'original insert', -97);
insert into tgt_test_short values ('101112', '1', 'original insert', -96);


/*this includes a delete, a multi-update, 
and an insert where the version is incorrect (it reused the original version number)*/
insert into src_test_short values ('123', '1st update', '2', 'U', '-99');
insert into src_test_short values ('456', '1st update', '2', 'U', '-98');
insert into src_test_short values ('456', '2nd update', '3', 'U', '-97');
insert into src_test_short values ('789', 'delete', '2', 'D', '-96');
insert into src_test_short values ('101112', 'accidental second insert', '1', 'I', '-95');
insert into src_test_short values ('131415', '1st insert', '1', 'I', '-95');


/*creates the two test tables*/
drop table if exists tgt_test_long;

create table tgt_test_long
    (
    id   varchar(50),
    version   varchar(10),
    txt_value   varchar(50),
    mismatch_col  int
    )
    sortkey(id)
;

drop table if exists src_test_long;

create table src_test_long
    (
    id   varchar(50),
    txt_value   varchar(50),
    version   varchar(10),
    dml_indicator varchar(10),
    mismatch_col  varchar(50)
    )
    SORTKEY(id)
;

/*inserts values into the target table.
This would be how the data looked on first insert, prior to updates coming in*/
insert into tgt_test_long values ('123', '1', 'original insert', -99);
insert into tgt_test_long values ('456', '1', 'original insert', -98);
insert into tgt_test_long values ('789', '1', 'original insert', -97);
insert into tgt_test_long values ('101112', '1', 'original insert', -96);


/*this includes a delete, a multi-update, 
and an insert where the version is incorrect (it reused the original version number)*/
insert into src_test_long values ('123', '1st update', '2', 'U', '-99');
insert into src_test_long values ('456', '1st update', '2', 'U', '-98');
insert into src_test_long values ('456', '2nd update', '3', 'U', '-97');
insert into src_test_long values ('789', 'delete', '2', 'D', '-96');
insert into src_test_long values ('101112', 'accidental second insert', '1', 'I', '-95');
insert into src_test_long values ('131415', '1st insert', '1', 'I', '-95');





drop table if exists tmp_number_series;
create TEMP table tmp_number_series
(
/*starts at 0 and increments by 1*/
/*use this if you want to be able to override the identity value*/
--sequence_number bigint identity(0,1) NOT NULL,
/*use this if you don't want to be able to override*/
sequence_number BIGINT GENERATED BY DEFAULT AS IDENTITY(0,1),
dummy_col smallint
)
distkey(sequence_number)
sortkey(sequence_number)
;

INSERT INTO tmp_number_series (dummy_col)
        select 1 as counter
        UNION ALL
        select 2 as counter
        UNION ALL
        select 3 as counter
        UNION ALL
        select 4 as counter
        UNION ALL
        select 5 as counter
        UNION ALL
        select 6 as counter
        UNION ALL
        select 7 as counter
        UNION ALL
        select 8 as counter
        UNION ALL
        select 9 as counter
        UNION ALL
        select 10 as counter
;

for cursor_var in 1..27 LOOP

    insert into tmp_number_series (dummy_col)
    select 1 from tmp_number_series
    ;

END LOOP;


INSERT INTO tgt_test_long (id, version, txt_value, mismatch_col)
SELECT
    nums.sequence_number,
    '1',
    'large initial load',
    '-70'
FROM
    tmp_number_series nums
;

INSERT INTO src_test_long (id, txt_value, version, dml_indicator, mismatch_col)
SELECT
    nums.sequence_number,
    '1st large batch',
    '2',
    'U',
    -70
FROM
    tmp_number_series nums
LIMIT 200000
;


--call sp_upsert_tester();
--select * FROM tgt_test_short;
--select * FROM src_test_short;
--select count(1) FROM src_test_long;
--select count(1) FROM tgt_test_long;
--call sp_upsert('tgt_test_short', 'id', 'version', 'src_test_short', 'id', 'version', 'dml_indicator');
--call sp_upsert('tgt_test_long', 'id', 'version', 'src_test_long', 'id', 'version', 'dml_indicator');
END;
$$ LANGUAGE plpgsql
SECURITY INVOKER;


