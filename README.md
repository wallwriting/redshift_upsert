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

- Join columns have to be of matching datatypes--I haven't made up my mind whether this should be enforced or should be flexible
- The matching columns between the source and target tables have to have the same names----I haven't made up my mind whether this should be enforced or should be flexible
- The DML indicator currently only recognizes the values 'I', 'U', and 'D'--this will likely have to be parameterized
- Currently requires all three matching fields (primary key, version id, dml indicator). Until this is addressed, since these are just column values, you can just plug in whatever column names you want as long as they exist in both source and target tables.
