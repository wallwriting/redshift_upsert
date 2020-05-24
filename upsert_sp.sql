CREATE OR REPLACE PROCEDURE upsert_sp(target_table INOUT character varying, target_key INOUT character varying, target_version INOUT character varying, source_table INOUT character varying, source_key INOUT character varying, source_version INOUT character varying, source_dml_indicator INOUT character varying)
/*(target_table, target_key, target_version, source_table, source_key, source_version)*/
AS $$
DECLARE 
    insert_var varchar(20);
    update_var varchar(20);

BEGIN
insert_var = 'I';
update_var = 'U';

EXECUTE
/*deletes any existing rows that will change in the batch*/
'DELETE
FROM
	 ' || target_table || 
' WHERE 
	 ' || target_key || ' IN
    /*gets target table_key that have a 
    version number lower than the source*/
    (
        SELECT
            maxtgt.' || target_key ||
        ' FROM

            /*target table id and max version number*/
            (
                SELECT
                     ' || target_key || ',
                    MAX(' || target_version || ') AS ' || target_version ||
                ' FROM
                     ' || target_table ||
                ' GROUP BY
                     ' || target_key ||
            ' ) maxtgt	
      JOIN
            /*source table*/
             ' || source_table || ' src
        ON 
            src.' || source_key || ' = maxtgt.' || target_key ||
            '/*only gets rows where the source
            timestamp is greater than the
            targets timestamp*/
            AND src.' || source_version || ' >= maxtgt.' || target_version ||
    ' )
;'
;
EXECUTE
'/*inserts source data into target table, only inserting new rows*/
INSERT INTO ' || target_table ||
    ' SELECT
		/*replace this with whatever you need to insert*/
			src.* 
	FROM
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
          		MAX(' || target_version || ') AS ' || target_version || 
          	' FROM '
          		 || source_table ||
			' WHERE 
          		/*DML indcator, must filter out
          		deletes prior to the GROUP BY*/
          		  ' || source_dml_indicator || ' IN(' || quote_literal(insert_var) || ', ' || quote_literal(update_var) || ') 
          	GROUP BY '
          		 || target_key || 
        ') mx
        ON src.' || source_key || ' = mx.' || target_key ||
        ' AND src.' || source_version || ' = mx.' || target_version ||
    ' LEFT JOIN '
         || target_table || ' tgt 
        ON src.' || source_key || ' = tgt.' || target_key || 
        ' AND src.' || source_version || ' = tgt.' || target_version || 
    ' WHERE 
        /*only inserts values not already present in target*/
        tgt.' || target_key || ' IS NULL
        AND src.' || source_dml_indicator || ' IN(' || quote_literal(insert_var) || ', ' || quote_literal(update_var) || ') 
    ;'

;

END;
$$ LANGUAGE plpgsql
SECURITY INVOKER;

