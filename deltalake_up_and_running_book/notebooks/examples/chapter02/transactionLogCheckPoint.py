# Databricks notebook source
# MAGIC %md # Delta Lake Example For Update Operation
# MAGIC ##### Tasks
# MAGIC 1. Write few records to Delta Lake
# MAGIC 1. Multiple 9 files in a loop 
# MAGIC 1. Add one more file to trigger checkpointing

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. WRITE FEW ROWS TO DELTA

# COMMAND ----------

# Create an array with the columns of our dataframe
columns = ['patientId', 'name', 'city']

# Create the data as an array of tuples
data = [
    (1, 'Patient 1', 'Seattle'),
    (2, 'Patient 2', 'Washington'),
    (3, 'Patient 3', 'Boston'),
    (4, 'Patient 4', 'Houston'),
    (5, 'Patient 5', 'Dallas'),
    (6, 'Patient 6', 'New York')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a delta file.
delta_path = f"{DA.paths.working_dir}/book/chapter02/TransactionLogCheckPoint"
df.coalesce(1).write.format("delta").save(delta_path)

#Create Hive Table for the data
spark.sql("DROP TABLE IF EXISTS patients_delta")
spark.sql("CREATE TABLE patients_delta USING DELTA LOCATION '{}'".format(delta_path))




# COMMAND ----------

# MAGIC %md **1.1: CHECK YOUR WORK**

# COMMAND ----------


from pyspark.sql.types import *

#See the data 
read_df = spark.read.format("delta").load(delta_path)
display(read_df)

#See the version history 
display(spark.sql("DESCRIBE HISTORY patients_delta"))

#See the files in delta lake folder
display(dbutils.fs.ls(delta_path))

#See the files in delta log folder
display(dbutils.fs.ls(f"{delta_path}/_delta_log"))

#See the content of the log file 
display(spark.read.json(f"{delta_path}/_delta_log/00000000000000000000.json"))

#Check your work
assert len(dbutils.fs.ls(delta_path)) > 0
assert read_df.count() == 6
print("All test pass")


# COMMAND ----------

# MAGIC %md **2: MULTIPLE WRITES IN A LOOP**

# COMMAND ----------

for index in range(9):
    # create a patient tuple
    patientId = 10 + index
    t = (patientId, f"Patient {patientId}", "Phoenix")
    
    # Create and write the dataframe
    df = spark.createDataFrame([t], columns)
    df.coalesce(1)             \
      .write                   \
      .format("delta")         \
      .mode("append")          \
      .save(delta_path)


# COMMAND ----------

# MAGIC %md **2.1: CHECK YOUR WORK**

# COMMAND ----------


from pyspark.sql.types import *

#See the data 
read_df = spark.read.format("delta").load(delta_path)
display(read_df)

#See the version history 
display(spark.sql("DESCRIBE HISTORY patients_delta"))

#See the files in delta lake folder
display(dbutils.fs.ls(delta_path))

#See the files in delta log folder
display(dbutils.fs.ls(f"{delta_path}/_delta_log"))

#See the content of the log file 
display(spark.read.json(f"{delta_path}/_delta_log/00000000000000000001.json"))

#Check your work
assert len(dbutils.fs.ls(delta_path)) > 0
print("All test pass")

# COMMAND ----------

# MAGIC %md **3: ADD ONE MORE FILE TO TRIGGER CHECKPOINTING**

# COMMAND ----------

patientId = 100
t = (patientId, f"Patient {patientId}", "Phoenix")

df = spark.createDataFrame([t], columns)
df.coalesce(1).write.format("delta").mode("append").save(delta_path)

# COMMAND ----------

# MAGIC %md **3.1: CHECK YOUR WORK**

# COMMAND ----------


from pyspark.sql.types import *

#See the data 
read_df = spark.read.format("delta").load(delta_path)
display(read_df)

#See the version history 
display(spark.sql("DESCRIBE HISTORY patients_delta"))

#See the files in delta lake folder
display(dbutils.fs.ls(delta_path))

#See the files in delta log folder
display(dbutils.fs.ls(f"{delta_path}/_delta_log"))

#See the content of the checkpoint parquet file 
display(spark.read.format("parquet").load(f"{delta_path}/_delta_log/00000000000000000010.checkpoint.parquet"))

#Check your work
assert len(dbutils.fs.ls(delta_path)) > 0
print("All test pass")


# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()
