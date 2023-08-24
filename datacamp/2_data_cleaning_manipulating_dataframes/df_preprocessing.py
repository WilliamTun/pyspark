
#============================
# FILTER and WHERE clause

# select specific_columns
filt_df = df.select('col_name1', 'col_name2')

# filter on column "col_name" where value begins with "M"
df.filter(df.col_name.like('M%'))

# filter / where
df.filter(df.int_col_name > 10)
df.filter(df.string_col_name == "hello")
df.filter(df['col_name'].isNotNull())
df.where(df.int_col_name > 10)
df.where(~ df.int_col_name.isNull())


# drop column
df.drop('col_name')



# ============================
# Apply functions to columns

import pyspark.sql.functions as F 

# apply uppper case 
df.withColumn("result_upper_case_col", F.upper("original_col_name"))

# create intermediary columns only for ETL
df.withColumn('result_splits_col', F.split("original_col_name", ","))

# change datatype of col
df.withColumn('result_new_col', F["original_col_name"].cast(IntergerType))


# ========================
# EXAMPLE - inspect data + filter 

# Show the distinct VOTER_NAME entries
voter_df.select("VOTER_NAME").distinct().show(40, truncate=False)

# Filter voter_df where the VOTER_NAME is 1-20 characters in length
voter_df = voter_df.filter('length(VOTER_NAME) > 0 and length(VOTER_NAME) < 20')

# Filter out voter_df where the VOTER_NAME contains an underscore
voter_df = voter_df.filter(~ F.col('VOTER_NAME').contains("_"))

# Show the distinct VOTER_NAME entries again
voter_df.select("VOTER_NAME").distinct().show(40, truncate=False)


# ==========================
# CONDITIONAL column operations

# conditional clauses

# 1. select with F.when()
#    F.when(<if condition>, <then x>)
import pyspark.sql.functions as F 
df.select(df.name, df.age, F.when(df.age >= 18, "adult"))
df.select(df.name, df.age, F.when(df.age < 18, "minor"))


# 2. select with F.when().otherwise()
df.select(df.name, df.age, F.when(df.age >= 18, "adult").otherwise("minor"))


