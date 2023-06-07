
# User defined functions

### example A 

# 1. define python function
def reverseString(x):
    return x[::-1]

#. 2. wrap function in udf and declare output type
udfReverseString = udf(reverseString, StringType())

#. 3. apply udf to df ... and specify name of new column to output to
user_df = df.withColumn('ReverseName', udfReverseString(df.Name))


### example B - udf with no arguments

def randomLetter():
    return random.choice(["A", "B", "C"])

udfRandomABC = udf(randomLetter, StringType()) 
userDF = user_df.withColumn("Letter", udfRandomABC())