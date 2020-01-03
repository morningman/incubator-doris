# SHOW PARTITIONS
## Description
This statement is used to display partition information
Grammar:
SHOW PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT];
Explain:
Support filter with following columns: PartitionId,PartitionName,State,Buckets,ReplicationNum,
LastConsistencyCheckTime

## example
1. Display partition information for the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name;

2. Display information about the specified partition of the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";

3. Display information about the newest partition of the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;

## keyword
SHOW,PARTITIONS

