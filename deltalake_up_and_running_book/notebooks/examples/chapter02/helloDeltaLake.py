# Databricks notebook source
# MAGIC %md # Delta Lake Lab
# MAGIC ##### Tasks
# MAGIC 1. Write numbers data to Delta
# MAGIC 1. Create table and view version history

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. Write few numbers to Delta
# MAGIC Write **`df`** to **`delta_path`**

# COMMAND ----------

delta_path = f"{DA.paths.working_dir}/book/chapter02/helloDeltaLake"

df = spark.range(0, 100000000)
df.write.format("delta").mode("overwrite").save(delta_path)                       

# COMMAND ----------

# MAGIC %md **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert len(dbutils.fs.ls(delta_path)) > 0

# COMMAND ----------

# MAGIC %md **2: CREATE TABLE FOR DATA**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS numbers_delta")
spark.sql("CREATE TABLE numbers_delta USING DELTA LOCATION '{}'".format(delta_path))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY numbers_delta"))

# COMMAND ----------

# MAGIC %md **2.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import *
numbers_delta_df = spark.sql("SELECT * FROM numbers_delta")
assert numbers_delta_df.count() == 100000000
assert numbers_delta_df.schema[0].dataType == LongType()
print("All test pass")

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()
