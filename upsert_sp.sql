CREATE OR REPLACE PROCEDURE sp_upsert(
                                target_table IN CHARACTER VARYING, 
                                target_key IN CHARACTER VARYING, 
                                target_version IN CHARACTER VARYING, 
                                source_table IN CHARACTER VARYING, 
                                source_key IN CHARACTER VARYING, 
                                source_version IN CHARACTER VARYING, 
                                source_dml_indicator IN CHARACTER VARYING
                                )
--DROP PROCEDURE upsert_sp(target_table IN CHARACTER VARYING, target_key IN CHARACTER VARYING, target_version IN CHARACTER VARYING, source_table IN CHARACTER VARYING, source_key IN CHARACTER VARYING, source_version IN CHARACTER VARYING, source_dml_indicator IN CHARACTER VARYING)

AS $$
DECLARE
    insert_var CHARACTER VARYING;
    update_var CHARACTER VARYING;
    col_nm_cursor RECORD;
    col_txt_var CHARACTER VARYING;
    colArrayVar CHARACTER VARYING;
    colArrayPrefixVar CHARACTER VARYING;
    finishInsertVar CHARACTER VARYING;
--    tmpTableName CHARACTER VARYING;

BEGIN

-- tmpTableName = 
--                 (
--                 SELECT
--                     'tmp_column_name_list_table_'
--                     ||                    
--                     REPLACE
--                         (CAST(pg_backend_pid() AS VARCHAR(25)) 
--                         || 
--                         CAST
--                             (CAST(EXTRACT('epoch' FROM current_timestamp) AS FLOAT)
--                                 AS VARCHAR(35)), '.', ''
--                         )
--                 )
-- ;



    /*Creates the temp table that will hold the column list. Table has arbitrary characters at the end
    to decrease the chance of duplicates. This value is not parameterized because it would require the
    queries to run AS an EXECUTE statement with quotes everywhere, like we are forced to do with the
    DELETE and INSERT statements. Convenience was traded for readability. To replace the table name,
    just do a find/replace in a text editor*/ 
    DROP TABLE IF EXISTS tmp_column_name_list_table_4383acad9Dk2;
    CREATE TEMP TABLE tmp_column_name_list_table_4383acad9Dk2
        (test_col VARCHAR(2500));
    INSERT INTO tmp_column_name_list_table_4383acad9Dk2 SELECT '';

    /*loops through the list of columns to 
    concatenate the column names into one list*/
    FOR col_nm_cursor IN 
                    /*gets column list FROM the system table
                    The join ensures only the matching column
                    names are involved in the upsert*/
                    SELECT 
                        tgt.column_name
                    FROM
                        (
                            SELECT
                                CAST(column_name AS VARCHAR(250)) AS column_name,
                                data_type
                            FROM 
                                information_schema.columns 
                            WHERE 
                                table_name = target_table
                            ORDER BY
                                1
                        ) tgt
                    JOIN
                        (
                            SELECT
                                CAST(column_name AS VARCHAR(250)) AS column_name,
                                data_type
                            FROM 
                                information_schema.columns 
                            WHERE 
                                table_name = source_table
                            ORDER BY
                                1
                        ) src
                        ON tgt.column_name = src.column_name
                        AND tgt.data_type = src.data_type
        LOOP
          /*converts the cursor to a string*/
          col_txt_var = col_nm_cursor;
        
          /*appends the column name list with the name FROM the current loop*/
          UPDATE tmp_column_name_list_table_4383acad9Dk2
          SET test_col = CASE 
                              /*skips the blank column name in the first pass*/
                              WHEN test_col = '' 
                                /*string function removes the leading and trailing parentheses*/
                                THEN 'src.'||substring(col_txt_var, 2, (SELECT len(col_txt_var) - 2)) 
                                /*adds a comma in between the values*/
                                ELSE test_col || ', ' || 'src.'||substring(col_txt_var, 2, (SELECT len(col_txt_var) - 2)) 
                          END;
        END LOOP;

        colArrayPrefixVar = (SELECT * FROM tmp_column_name_list_table_4383acad9Dk2);
        /*a second version of the column list that removes the table prefix*/
        colArrayVar = (SELECT REPLACE(colArrayPrefixVar, 'src.', ''));

/*This ends the column list generating section*/



/*hard codes the dml indiciator--this will eventually need to be flexible*/
insert_var = 'I';
update_var = 'U';

/*for the end of the insert statement, this will create different lines based on
whether the call passed a real dml indicator or a value of X*/
IF UPPER(source_dml_indicator) = 'X' THEN finishInsertVar = '1=1';
    ELSE finishInsertVar = 'src.' || source_dml_indicator || ' IN(' || quote_literal(insert_var) || ', ' || quote_literal(update_var) || ')';
END IF;

/*for the end of the delete statement, this will create different lines based on
whether the call passed a real version id or the same value AS the key*/
-- IF target_version = target_key THEN finishDeleteVar = '1=1';
--     ELSIF source_version = source_key THEN finishDeleteVar = '1=1';
--     ELSE finishDeleteVar = 'src.'||source_version || ' >= maxtgt.tgt_version_col';
-- END IF;



/*This starts the actual delete and insert*/
EXECUTE
'/*deletes any existing target rows that will change in the batch*/
DELETE
FROM
	 ' || target_table || 
' WHERE 
	 ' || target_key || ' IN
    /*gets target table_key that have a version number lower than the source*/
    (
        SELECT
            maxtgt.'||source_key ||
         ' FROM
            ' || source_table || ' as maxtgt 
    )
;'
;


EXECUTE
    '/*In cases where there are multiple transactions in the incoming batch
    this will delete all but the latest record*/
    DELETE FROM ' || source_table || '
    WHERE EXISTS
        (     
                SELECT 1
                FROM
                    /*This finds the max version number for each id*/
                    (
                        SELECT
                            src.' || source_key || ', 
                            MAX(src.' || source_version || ') AS max_version_col  
                        FROM '
                            || source_table || ' src 
                        GROUP BY '
                            || source_key ||  ' 
                    ) mx
                  WHERE ' 
                      || source_table || '.' || source_key || ' = mx.' || source_key || '  
                      /*deletes anything that is not the max version number*/
                      AND ' || source_table || '.' || source_version || ' != mx.max_version_col 
        )
    ;'

;


EXECUTE
    '/*inserts source data into target table, only inserting new rows*/
    INSERT INTO ' || target_table || '
        (' || colArrayVar || ') 
        SELECT 
        /*replace this with whatever you need to insert*/ '
        || colArrayPrefixVar || ' 
        FROM
        /*source*/ ' 
        || source_table || ' src
        WHERE 
            /*only inserts inserts and updates*/ ' 
            || finishInsertVar || ' 
    ;'

;

END;
$$ LANGUAGE plpgsql
SECURITY INVOKER;
