

# find number of unique rows in df and cache 
df = df.distinct().cache()
# count unique rows : takes long time first attempt 
df.count() 
# count unique rows again : fast operation second attempt because of caching.
df.count()

# 1. check if df is cached
df.is_cached()
# 2. remove dataframe from cache after usage to prevent excess memory usage on cluster
df.unpersist()
# 3. check if df is cached
df.is_cached()


