

# Printing the execution plan

```
df - df.select(df["col1"]).distinct()
df.explain()
```


# Shuffling
spark distributes data to many workers
the side effect is data rows gets "shuffled"
and shuffling operations slow processing

How to limit shuffling?

1. limit use of df.repartition(num_partitions)
   instead use: df.coalesce(num_partitions)

2. df.join() is useful but can cause shuffling. 
    Limit it's usage to only when necessary

3. use df.broadcast()
   broadcast provides a copy of an object to a worker
   if a worker has an object copy, it does not need to communicate to other nodes. 
   can speed up join operations. 


```
from pyspark.sql.functions import broadcast
combined_df = df1.join(broadcast(df2))
```

note. if df2 is a very small df, using broadcast may actually SLOW down operations


