# Databricks notebook source
# MAGIC %md # Delta Lake Example For Update Operation
# MAGIC ##### Tasks
# MAGIC 1. Write numbers data to Delta
# MAGIC 1. Create table and view version history

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. Write few patients information to Delta
# MAGIC Write **`df`** to **`delta_path`**

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

# COMMAND ----------

# MAGIC %md **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert len(dbutils.fs.ls(delta_path)) > 0
df = spark.read.format("delta").load(delta_path)
df.show()

# COMMAND ----------

# MAGIC %md **2: CREATE TABLE FOR DATA**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS patients_delta")
spark.sql("CREATE TABLE patients_delta USING DELTA LOCATION '{}'".format(delta_path))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY patients_delta"))

# COMMAND ----------

# MAGIC %md **2.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import *
patients_delta_df = spark.sql("SELECT * FROM patients_delta")
assert patients_delta_df.count() == 4
print("All test pass")

# COMMAND ----------

# MAGIC %md **3: APPEND MORE PATIENTS**

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

# MAGIC %md **3.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import *
assert patients_delta_df.count() == 6
patients_delta_df.show()
display(spark.sql("DESCRIBE HISTORY patients_delta"))
print("All test pass")

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()
