

# EXAMPLE 1: read parquet
df = spark.read.format('parquet').load('filename.parquet')
df = spark.read.parquet('filename.parquet')

# EXAMPLE 2: writing parquet
df.write.format('parquet').save('filename.parquet')
df.write.parquet('filename.parquet')


# EXAMPLE 3: Read in parquet and apply SQL

flight_df = spark.read.parquet('flights.parqueet')
flight_df.createOrReplaceTempView('flights_table')
short_flights_df = spark.sql('SELECT * FROM flights_table WHERE flight_duraction < 100')


# =========
# EXAMPLE 4: check row counts, combine dataframes, save and read parquet
# View the row count of df1 and df2
print("df1 Count: %d" % df1.count())
print("df2 Count: %d" % df2.count())
# Combine the DataFrames into one
df3 = df1.union(df2)
# Save the df3 DataFrame in Parquet format
df3.write.parquet('AA_DFW_ALL.parquet', mode='overwrite')
# Read the Parquet file into a new DataFrame and run a count
print(spark.read.parquet('AA_DFW_ALL.parquet').count())


# EXAMPLE 5: read parquet, SQL query and collect
# Read the Parquet file into flights_df
flights_df = spark.read.parquet('AA_DFW_ALL.parquet')
# Register the temp table
flights_df.createOrReplaceTempView('flights')
# Run a SQL query of the average flight duration
avg_duration = spark.sql('SELECT avg(flight_duration) from flights').collect()[0]
print('The average flight time is: %d' % avg_duration)