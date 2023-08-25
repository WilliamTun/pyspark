'''
Overview

Data schemas are important in spark
as they:
1. Define the formats of each column in a dataframe
2. Filter out garbage data on import.
3. High performance... Data schemas improve read performance.
4. orderly data flows.

'''

from pyspark.sql.types import *

mySchema = StructType([
    StructField('col1', StringType(), True),
    StructField('col2', IntegerType(), True),
    StructField('col3', StringType(), True)
])

# read csv
df = spark.read.format('csv').load(name='data.csv', schema=mySchema)

