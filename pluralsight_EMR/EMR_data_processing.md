# Connecting to other AWS services

- EMR has connectors for other aws services
  eg. DynamoDB / Kinesis streams
- Can use "HIVE" within EMR, to connect to DynamoDB 
  and perform operations: loading data to EMR / exporting to S3 / redshift


# Export data to redshift
- use COPY command 
- uses ssh with key authentication from master node
- requires correct IAM permissions


# HBase and EMR
-  HBase is an open source non-relational distributed database as part of hadoop project
- runs on top of HDFS
- is a NoSQL database like DynamoDB
- allows direct input / output to MapReduce framework
- can storee a clusters Hbase root directory & metadata into S3
- supports READ-ONLY access to clusters data
- Hbase can create data snapshots and output to S3 for data recovery in case of disaster

# PrestoDB
- Opensource distributed SQL engine
- queries data where the data is stored ...
  so we do not have to move data to a separate analytics system
- fast analytic queries
- works well with HDFS (hadoop distributed file system) / S3 / Hbase / mySQL / Redshift