
# ==== inefficient join =======
# normal join
new_df = df1.join(df2, df1["key1"] == df2["key2"])
# show execution plan
new_df.explain()

# ==== efficient join via broadcast ==========
'''
broadcast is used to give each node a copy of the specified data
tip: broadcast the smaller dataframe out of the two to join
tip: for verysmall datasets, we do not need broadcasting
'''
from pyspark.sql.functions import broadcast 

broadcast_df = df1.join(broadcast(df2), df1["key1"] == df2["key2"])

# the presence of a broadcastHashJoin indicates you have successfully implemented broadcast
broadcast_df.explain()

