# redshift_upsert
This creates a stored procedure that runs an upsert to a table

This is intended to be a "last mile" process, meaning the rest of the transformation has aleady happened (either in database or via a separate ETL engine), with the results loaded into a staging table that has the same (or nearly the same) schema as the target. In order to do so, it assumes you have the following information:
  - Primary Key
  - Version Id (eg a sequence number or a timestamp)--this handles cases where the same record has multiple updates
  - DML indicator (eg 'I' for insert, 'U' for update, 'D' for delete)

Some of these can be derived, like using a file create timestamp for the version id, but all three are necessary in order to do a merge or upsert, otherwise compromises will have to be made:
  - missing the DML indicator means you will not be able to tackle deletes, only inserts and updates
  - missing the version id means you can't handle situations where the source row gets updated more than once per batch
  - missing the primary key means you can only do inserts

In other words, if you don't have these three attributes, you're going to have problems whether you use this stored procedure or any tool to handle merges.

The stored procedure has the following parameters:
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


There are limitations that need to be addressed:

- Join columns have to be of matching datatypes*
- The matching columns between the source and target tables have to have the same names*
- You can only have a single column key*+
- There is currently no error handling of any kind
- The DML indicator currently only recognizes the values 'I', 'U', and 'D'--this will likely have to be parameterized
- Currently requires all three matching fields (primary key, version id, dml indicator) to have arguments passed. Use these workarounds when you don't have them:
    - If there is no dml indicator, pass the value 'X' for the argument (case doesn't matter).
    - If there is no version id, pass the same vaule for key.**
- Currently doesn't take advantage of the sort key for performance. A feature will have to be added where it checks the system table for a sort key and passes the min/max value of that column from the staging table into the DELETE query in order to leverage zone maps. The sort key should be irrelevant for the INSERT portion of the proc.

[* I'm still undecided whether there are problems or are actually things that should be enforced as a best practice]

[+ As a workaround, you can manufacture a column in both the stage and target tables that concatenate the compound key]

[** If the source system sends multiple transactions on the same record within one batch AND it doesn't provide a version id,
this will insert all versions of the record into the target. Frankly, this situation shouldn't be happening. You need to have a conversation with the team that runs the source system and tell them you need some type of versioning. Otherwise the only option is to subquery the source table with a MAX() on all non-key columns, GROUP BY the key column. This will at least avoid duplicates, but there is no way to know if the correct version of the record was selected for insert. This is not a flaw in the code; it is a flaw in the operational process. Putting he MAX() function in the main branch script would negatively impact performance for the normal case just to provide a workaround for this exception case. For the time being, this will not be included in the main branch. After AQUA is released, this will be revisited as that feature could mitigate performance issues with the MAX().]
