# SHOW PARTITIONS
## description
    该语句用于展示分区信息
    语法：
        SHOW PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT];
    说明:
        支持PartitionId,PartitionName,State,Buckets,ReplicationNum,LastConsistencyCheckTime等列的过滤 

## example
    1.展示指定db下指定表的所有分区信息
        SHOW PARTITIONS FROM example_db.table_name;
        
    2.展示指定db下指定表的指定分区的信息
        SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    
    3.展示指定db下指定表的最新分区的信息        
        SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
## keyword
    SHOW,PARTITIONS
