'''

Python variables are mutable
- which is a problem for computations with concurrency

Spark dataframes are immutable
- dataframes are defined once and not mutable after initialisation
- spark is designed to use immutable objects
- if a variable name is reused, the original data is removed 
  and a new data object is assigned to that variable
- immutable objects allow safely sharing data across cluster components
-  
'''


df = spark.read.csv("data.csv")

# Changing column names
df = df.withColumn("col1", "new_col")
# drop old column 
df.drop(df.col1)

# Changing column names
df = df.withColumn("col2", df.col1 + 2000)
# drop old column 
df.drop(df.col2)


# ========= EXAMPLE 2 ===============

# alternative way to read data
df = spark.read.format("csv").options(Header=True).load("data.csv")
# create new lowercase column
df = df.withColumn("new_col", F.lower(df['col']))
df = df.drop(df['col'])
df.show()



