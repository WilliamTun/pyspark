
# caching

.cache()

```
df = spark.read.csv("file.txt.gz")
df.cache().count()

# check if df is cached
print(df.is_cached)

# uncach dataframe
df.unpersist()
```


Pros:
- store dataframes in memory or disk of processing nodes
- improves speed on transformations as data no longer needs to be retrieved from original data source
- reduces resource usage

Cons:
- very large dataseets may not fit in memory

# Tips
- choice of WHERE to cache is important
- cache in memory or fast SSD / NVMe storage
- cache to slow local disk if needed for big data
- If caching does not work, create intemediate parquet files
- STOP caching an object once it is fully used up to free resources

# when to use cache?
- when results of transformation needs to be used several times 
- try caching at different points and run tests to see if performance improves
