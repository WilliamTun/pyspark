
# "Handling and analysing data with ElasticMapReduce"
https://app.pluralsight.com/course-player?clipId=62383f93-458f-44a4-9cf8-d4c6545768a3

# EMR
- managed cluster for big data anlaytics
- can move or transform data in & out of aws databases
  eg. dynamoDB / s3


# EMR architecture
- storage layer : Hadoop File system / EMR file system / local file systems
- cluster resource management : YARN for scheduling jobs 
- data processing framework : 
    1. batch processing 
    2. streaming processing
    3. in memory processing 
    frameworks:
    a) hadoop 
    b) apache spark
- application & programming layer 



# EMR-FS (EMR file system)
- AWS version of Hadoop file system
- used by EMR clusters to read / write to S3
- acts as persistent storage
- enables use of data encryption

