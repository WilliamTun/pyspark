

# ====== partitions ========
# 1. Dataframes are broken into partitions
# 2. Partions vary in size
# 3. Each parition is handled independently, 
#    allowing for workload distribution across multiple clusters.
 

 # ===== lazy transforms ==========
 # 1. all transformations in spark are lazy
 #    meaning spark does not perform operations 
 #    but rather plans out their executions.
 #    via DAGS.
 #    instead, it is the workers that evaluate the plan later.
 # 2. No transformation or operation is done
 #    until an ACTION is performed
 #    eg. count() / write()
 # 3. Transformations can be re-ordered for best performance
 #    which can cause confusion  