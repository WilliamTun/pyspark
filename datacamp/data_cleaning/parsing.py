

# remove commented rows
df = spark.read.csv("file.csv.gz", comment="#")

# make first line header of df
df = spark.read.csv("file.csv.gz", header='True')

# define separators (default sep = ",")
df = spark.read.csv("file.csv.gz", sep='\t')

