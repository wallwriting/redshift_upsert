# redshift_upsert
This creates a stored procedure that runs an upsert to a table

The stored proc assumes that the transformation process has already happened, and that the results were loaded into a staging table (the source) that has the same schema as the target table.

It requires the table have the following attributes:
  - Primary Key
  - Version Id (eg a sequence number or a timestamp)
  - DML indicator (eg 'I' for insert, 'U' for update, 'D' for delete)

It has the following parameters:
  - target table name
  - target primary key column name
  - target version id column name
  - source table name
  - source primary key column name
  - source version id column name
  - source DML indicator column name

It can handle deletes only if the source system sends the delete record

It can handle multiple updates to the same record in a single batch.

It can't handle a situation where the primary key is updated in the source unless the source system sends two records: a delete with the original key and an insert (an update will work too) with the new key

Copy the contents of upsert_sp.sql into a Redshift query editor (remember that for editors that don't run all statements in batch by default, you need to make sure to change the query run option so that the entire proc is created by running the script in batch).

Copy the contents of upsert_tester.sql to create a sample source and target table with sample data in order to test out the procedure. The upsert_tester.sql also has the call statement to run the proc against the test tables.


This is a work in progress. Currently, there are limitations that need to be improved on before this can be used in a real-world environment:

- All columns have to be text when in reality some of the applicable fields will be integer or timestamp
- The target table has to have the exact same schema as the source--for example, this assumes the target table includes the DML indicator when a real target table would not--because the INSERT uses a SELECT statement with a wildcard; this will eventually have to be changed... somehow... maybe with parameters? I've tried using the system views/tables to identify the list of column names, but that doesn't seem viable as they can't be used in any query that has a segment that needs to run on the comopute node (ie every query) because the system views/tables are in the leader node
- The DML indicator currently only recognizes the values 'I', 'U', and 'D'--this will likely have to be parameterized
