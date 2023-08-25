

# Option 1: use udfs
- user defined functions


# Option 2: Inline functions
```
df = df.read.csv("file.csv")
df = df.withColumn("avg", (df.total/df.count))
df = df.withColumn("surface", df.width * df.length)
df = df.withColumn("avg_size", udfComputeTotal(df.entries)/df.num)
```