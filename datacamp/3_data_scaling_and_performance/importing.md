
# Spark processes

Two types:
1. Driver processes
2. Worker processes

# Importing data to spark DFs
important parameters
1. number of objects
   importing more objects is better than large objects
   as we can distribute "import" across workers
2. import via wildcards?
   ```
    df = spark.read.csv("file-*.txt.gz")
   ```
3. performance increases if size of objects
   to import are of similar size
4. use well-defined schemas for import
    + provides validation during import


# How to split files into multiple objects

1. Method 1:  OS utilities: split / cut / awk 
   eg.
    ``` 
    split -l 10000 -d largefile chunk-
    ```
2. Method 2: custom scripts eg. python
3. Method 3: write out to parquet


