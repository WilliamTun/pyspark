# Partitioning 

# 1. dataframes are broken into partitions
# 2. partions can vary in size
# 3. each partition is handled independently. 


# Lazy processing

# 1. most transformations are lazy in spark
#    we define what should be done
#    but execution does not take place until needed
# 2. eg. df.withColumn() 
#    eg. df.select()
#    eg. df.count() / df.count()



# Concept: 
# In relational datbases, most tables have IDs
# IDs are: 
#     1. usually integers
#     2. increment in value sequentially 
#     3. unique
# Such ID's are difficult to parallelise

# Spark solution to ID's: 
pyspark.sql.functions.monotonically_increasing_id()

# sparks monotonically increasing IDs:
#    1. are still unique and incrementally increasing in value
#    2. are parallelizable
#    3. may not be in sequential order (eg. 1, 2, 3 ... n)
#    4. the id's are randomly assigned and so gaps may exist in id values



