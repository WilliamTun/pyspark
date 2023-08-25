

# ==== read in cluster summary stats spark configurations =====
app_name = spark.conf.get("spark.app.name")
driver_tcp_port = spark.conf.get("spark.driver.port")
num_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Name {app_name}")


# ==== changing spark configurations ====
num_partitions_before = df.rdd.getNumPartitions()
# change configurations
spark.conf.set('spark.sql.shuffle.partitions', 500)
df = spark.read.csv('data.txt.gz').distinct()
num_partitions_after = df.rdd.getNumPartitions()
