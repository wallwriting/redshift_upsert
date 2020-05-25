CREATE OR REPLACE PROCEDURE upsert_sp(target_table INOUT character varying, target_key INOUT character varying, target_version INOUT character varying, source_table INOUT character varying, source_key INOUT character varying, source_version INOUT character varying, source_dml_indicator INOUT character varying)
--drop procedure PROCEDURE upsert_sp(target_table INOUT character varying, target_key INOUT character varying, target_version INOUT character varying, source_table INOUT character varying, source_key INOUT character varying, source_version INOUT character varying, source_dml_indicator INOUT character varying)
 
AS $$
DECLARE
    insert_var character varying;
    update_var character varying;
    col_nm_cursor record;
    col_txt_var character varying;
    colArrayVar character varying;
    colArrayPrefixVar character varying;
    finishInsertVar character varying;
    finishDeleteVar character varying;
BEGIN
    /*creates the temp table that will hold the column list*/
    drop table if exists tmp_column_name_list_table_4383acad9Dk2;
    create temp table tmp_column_name_list_table_4383acad9Dk2
        (test_col varchar(2500));
    insert into tmp_column_name_list_table_4383acad9Dk2 select '';

    /*loops through the list of columns to 
    concatenate the column names into one list*/
    FOR col_nm_cursor IN 
                    /*gets column list from the system table
                    The join ensures only the matching column
                    names are involved in the upsert*/
                    SELECT 
                        tgt.column_name
                    FROM
                        (
                            SELECT
                                cast(column_name as varchar(250)) as column_name
                            from 
                                information_schema.columns 
                            WHERE 
                                table_name = target_table
                        ) tgt
                    JOIN
                        (
                            SELECT
                                cast(column_name as varchar(250)) as column_name
                            from 
                                information_schema.columns 
                            WHERE 
                                table_name = source_table
                        ) src
                        ON tgt.column_name = src.column_name
        LOOP
          /*converts the cursor to a string*/
          col_txt_var = col_nm_cursor;
        
          /*appends the column name list with the name from the current loop*/
          update tmp_column_name_list_table_4383acad9Dk2
          set test_col = CASE 
                              /*skips the blank column name in the first pass*/
                              WHEN test_col = '' 
                                /*string function removes the leading and trailing parentheses*/
                                THEN 'src.'||substring(col_txt_var, 2, (select len(col_txt_var) - 2)) 
                                /*adds a comma in between the values*/
                                ELSE test_col || ', ' || 'src.'||substring(col_txt_var, 2, (select len(col_txt_var) - 2)) 
                          END;
        END LOOP;

        colArrayPrefixVar = (select * FROM tmp_column_name_list_table_4383acad9Dk2);
        /*a second version of the column list that removes the table prefix*/
        colArrayVar = (SELECT REPLACE(colArrayPrefixVar, 'src.', ''));


/*This ends the column list generating section*/

insert_var = 'I';
update_var = 'U';

/*for the end of the insert statement, this will create different lines based on
whether the call passed a real dml indicator or a value of X*/
IF UPPER(source_dml_indicator) = 'X' THEN finishInsertVar = '1=1';
    ELSE finishInsertVar = 'src.' || source_dml_indicator || ' IN(' || quote_literal(insert_var) || ', ' || quote_literal(update_var) || ')';
END IF;

/*for the end of the delete statement, this will create different lines based on
whether the call passed a real version id or the same value as the key*/
IF target_version = target_key THEN finishDeleteVar = '1=1';
    ELSIF source_version = source_key THEN finishDeleteVar = '1=1';
    ELSE finishDeleteVar = 'src.'||source_version || ' >= maxtgt.tgt_version_col';
END IF;



/*This starts the actual delete and insert*/
EXECUTE
/*deletes any existing rows that will change in the batch*/
'DELETE
FROM
	 ' || target_table || 
' WHERE 
	 ' || target_key || ' IN
    /*gets target table_key that have a version number lower than the source*/
    (
        SELECT
            maxtgt.'||target_key ||
        ' FROM
            /*target table id and max version number*/
            (
                SELECT
                     ' || target_key || ',
                    MAX(' || target_version || ') AS tgt_version_col 
                FROM
                     ' || target_table ||
                ' GROUP BY
                     ' || target_key ||
            ' ) maxtgt	
      JOIN
            /*source table*/
             ' || source_table || ' src
        ON 
            src.'||source_key || ' = maxtgt.'||target_key ||
            '/*only gets rows where the source timestamp is greater than the target timestamp*/
            AND ' || 
            finishDeleteVar
--'src.'||source_version || ' >= maxtgt.tgt_version_col'
    ||' )
;'
;
EXECUTE
'/*inserts source data into target table, only inserting new rows*/
INSERT INTO ' || target_table ||
    ' (' || colArrayVar || ') 
     SELECT
		/*replace this with whatever you need to insert*/
		 ' || colArrayPrefixVar ||
	' FROM
		/*source*/
		 ' || source_table || ' src
	/*gets the most recent change for a given record
    in case multiple updates were made at the source
    in between batches*/
	JOIN
    		(
			/*Three key elements: record identifier, versioning column, DML indicator*/
          	SELECT
				/*the record identifier*/
          		  ' || target_key || ', 
				/*most recent version*/
          		MAX(' || target_version || ') AS tgt_version_col  
          	FROM '
          		 || source_table || ' src 
			 WHERE 
          		/*DML indcator, must filter out deletes prior to the GROUP BY*/ ' ||
              finishInsertVar
--                'src.'||source_dml_indicator || ' IN(' || quote_literal(insert_var) || ', ' || quote_literal(update_var) || ')'
          	|| ' GROUP BY '
          		 || target_key || 
        ') mx
        ON src.'||source_key || ' = mx.'||target_key ||
        ' AND src.'||source_version || ' = mx.tgt_version_col 
    LEFT JOIN '
         || target_table || ' tgt 
        ON src.'||source_key || ' = tgt.'||target_key || 
        ' AND src.'||source_version || ' = tgt.'||target_version || 
    ' WHERE 
        /*only inserts values not already present in target*/
        tgt.'||target_key || ' IS NULL AND ' ||
        finishInsertVar
--        'src.'||source_dml_indicator || ' IN(' || quote_literal(insert_var) || ', ' || quote_literal(update_var) || ')'
    ||';'

;

END;
$$ LANGUAGE plpgsql
SECURITY INVOKER;