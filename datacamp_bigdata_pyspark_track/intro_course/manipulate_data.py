

### ========== Create a new column in spark ==============
# Create the DataFrame flights
df = spark.table("flights")

# Show the head
df.show()

def func(x):
    return x + 1

# Add new column 
df = flights.withColumn("new_col_name", func(df.<col_name>))


### ========= FILTER ====================
# Filter by passing a string query
df1 = df.filter("col_name > 1000")

# Filter by passing a column of boolean values
df2 = df.filter(df.col_name > 1000

### ========= SELECT =====================
# Select method 1
selected1 = df.select("col1", "col2", "col3")

# Select method
temp = df.select(df.col1, df.col2, df.col3)

# Define first filter
filterA = df.col1 == "A"

# Define second filter
filterB = df.col2 == "B" 

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

### ========== SELECT and TRANSFORM with ALIAS  ====================

# Define avg_speed
new_col = (df.col1/(df.col2/60)).alias("col3")

# Select the correct columns
speed1 = df.select("col1", "col2", "col3", new_col)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")


### ========== AGGREGATION ==========================================

# Find the shortest flight from PDX in terms of distance
flights.filter(df.origin == "PDX")
   .groupBy()
   .min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA")
    .groupBy()
    .max("air_time").show()

# Average duration of Delta flights from SEA
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air in hours
# create a new col called: duration_hrs
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()

# =============== GROUP and AGGREGATION ========================

# GROUPBY tailnum
by_plane = flights.groupBy("tailnum")
# COUNT Number of flights each plane made
by_plane.count().show()

# GROUPBY origin
by_origin = flights.groupBy("origin")
# AVERAGE duration of flights from PDX and SEA
by_origin.avg("air_time").show()


# =============== GROUP and AGGREGATION 2 - with F ========================
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()


### ========== JOIN ==========================================
# A join will combine two different tables along a column that they share. This column is called the key


# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# LEFT-OUTER-JOIN the DataFrames on shared column name "dest"
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
print(flights_with_airports.show())