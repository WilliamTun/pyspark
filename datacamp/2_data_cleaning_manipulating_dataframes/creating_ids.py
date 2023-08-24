
# pyspark.sql.functions.monotonically_increasing_id()

# - creates 64 bit integer values 
#   which are unique and increasing in value
# - numbers may appear random and not sequential
# - These ids can be completely parallelised
#   and suit adding to datasets that need 
#   to be transformed through distributed clusters