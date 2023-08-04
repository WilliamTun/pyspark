# At the core of the pyspark.ml module are:
# 1. Transformer 
# 2. Estimator classes. 

'''
 Transformer classes:
# - have a .transform() method 
# - INPUT: DataFrame 
#   OUTPUT: new DataFrame - usually the original one with a new column appended. 
# - For example, you might use the class Bucketizer 
#   to create discrete bins from a continuous feature 
# - For example the class PCA,
#   to reduce the dimensionality of your dataset 
#   using principal component analysis.

# Estimator classes
# - have a .fit() method. 
# - INPUT: DataFrame, 
# - OUTPUT: model object
# - Example:
#   1. StringIndexerModel : for including categorical data saved as string
#   2, RandomForestModel : for classification or regression.
'''



# ====== RENAME & JOIN =============
# Rename year column
planes = planes.withColumnRenamed("year", "plane_year")

# Join two DataFrames
model_data = flights.join(planes, on="tailnum", how="leftouter")


'''
DATA TYPES
- Spark only handles numeric data (ints / floats / doubles)
- spark can infer a number from a string-number
  but can get dtypes wrong
- you can specify dtypes via .withColumn() and .cast() 
- .cast("integer")
- .cast("double")
'''

# ======= CAST ========
# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time", model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month", model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))

# ======== OPERATIONS ON NUMERICS + CREATE COLUMN
# Create the column plane_age by subtracting plane_year from year
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)


# ======== REMOVE NAN
# Convert col to an integer
model_data = data.withColumn("label", data.column_name.cast("integer"))

# Remove missing values
model_data = model_data.filter("col1 is not NULL and col2 is not NULL")


'''
Strings and factors

- Spark requires numeric data for modeling.
- how to deal with string categories?
- pyspark.ml.features submodule 
  can create what are called 'one-hot vectors' 
  where every observation has a vector 
  in which all elements are zero except for at most one element (1)
- STEP 1:
  create StringIndexer to encoding your categorical feature
  Members of this class are Estimators 
  INPUT: DataFrame 
  LOGIC: map each unique string to a number. 
  OUTPUT: Transformer 
          input: DataFrame
          logic: attaches the mapping to it as metadata
          output: new DataFrame with a numeric column corresponding to the string column.
- STEP 2:
  OneHotEncoder:
  encode this numeric column as a one-hot vector
  works like StringIndexer
  & creates an Estimator and then a Transformer. 
  
  OUTPUT: 
  a column that encodes your categorical feature as a vector
that's suitable for machine learning 


SUMMARY:
All you need to do to handle string columns in ML applications is:
1. create a StringIndexer 
2. create a OneHotEncoder
... and the Pipeline will take care of the rest.
''' 

# Create a StringIndexer
carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")

# Create a OneHotEncoder
carr_encoder = OneHotEncoder(indexCol="carrier_index", outputCol="carrier_fact")