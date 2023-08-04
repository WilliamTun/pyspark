# source track: https://app.datacamp.com/learn/skill-tracks/big-data-with-pyspark


# source tutorial: https://campus.datacamp.com/courses/introduction-to-pyspark/getting-to-know-pyspark?ex=2

# Get started with spark: SparkContext
1. Connecting spark to a cluster - by creating a SparkContext class.

2. In production, the cluster is hosted on a master machine, that is connected to many worjer node machines. The master manages splitting the data. The result of the workers return to master.

3. In development, you can run spark locally. 


```
sc = SparkConf() 

#verify version
print(sc.version)
```


# Spark Dataframes

- The core spark data structure is RDD
- it is difficult to work with low level RDDs directky
- use spark DF to make working with spark easier
- sparkDF works like SQL table
- sparkDF automates optimising operation speed for you. 
- sparkContext connects to cluster
- sparkSession interfacts with sparkContext

```
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)


# Your SparkSession has an attribute called catalog which lists all the data inside the cluster. 
# Print the tables in the catalog
print(spark.catalog.listTables())
```

# SparkSQL
- you can run SQL queries on spark dataframes or any tables already in the spark clustre. 

```
# sql query to select top 10 rows
query = "FROM <table> SELECT * LIMIT 10"

# Get the first 10 rows
out = spark.sql(query)

# Show the results
out.show()
```

# SparkDF to pandas

```
# Convert the results to a pandas DataFrame
pd_counts = out.toPandas()
```

# pandas to SparkDF

```
# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())
```

# spark read csv
```
# Don't change this file path
file_path = "/usr/local/share/path/file.csv"

# Read in the airports data
df = spark.read.csv(file_path, header=True)

# Show the data
df.show()
```
