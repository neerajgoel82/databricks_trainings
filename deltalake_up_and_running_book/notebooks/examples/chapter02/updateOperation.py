# Databricks notebook source
# MAGIC %md # Delta Lake Example For Update Operation
# MAGIC ##### Tasks
# MAGIC 1. Write few records to Delta Lake
# MAGIC 1. Add more records to Delta Lake 
# MAGIC 1. Update a record in Delta Lake 

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. WRITE FEW ROWS TO DELTA

# COMMAND ----------

# Create an array with the columns of our dataframe
columns = ['patientId', 'name']

# Create the data as an array of tuples
data = [
    (1, 'P1'),
    (2, 'P2'),
    (3, 'P3'),
    (4, 'P4')
]

delta_path = f"{DA.paths.working_dir}/book/chapter02/UpdateOperation"

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(2)        \
  .write              \
  .format("delta")    \
  .mode("overwrite")  \
  .save(delta_path)

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
assert read_df.count() == 4
print("All test pass")


# COMMAND ----------

# MAGIC %md **2: APPEND MORE PATIENTS**

# COMMAND ----------

# Create the data as an array of tuples
new_data = [
    (5, 'P5'),
    (6, 'P6')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(new_data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(1).write.format("delta").mode("append").save(delta_path)


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
assert read_df.count() == 6
print("All test pass")

# COMMAND ----------

# MAGIC %md **3: UPDATE A PATIENT**

# COMMAND ----------

from delta import *
from delta.tables import *
from pyspark.sql.functions import lit

deltaTable = DeltaTable.forPath(spark, delta_path)
            
deltaTable.update(
  condition = "patientId = 1",
  set = { 'name': lit("p11")}
)

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

#See the content of the log file 
display(spark.read.json(f"{delta_path}/_delta_log/00000000000000000002.json"))

#Check your work
assert len(dbutils.fs.ls(delta_path)) > 0
assert read_df.count() == 6
print("All test pass")


# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()
