
full_df = spark.read.csv("data_full.txt.gz")
# read in many split files with wild card
split_df = spark.read.csv("data_*.txt.gz")