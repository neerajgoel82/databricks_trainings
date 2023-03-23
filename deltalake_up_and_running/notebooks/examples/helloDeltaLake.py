# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Delta Lake Lab
# MAGIC ##### Tasks
# MAGIC 1. Write sales data to Delta
# MAGIC 1. Modify sales data to show item count instead of item array
# MAGIC 1. Rewrite sales data to same Delta path
# MAGIC 1. Create table and view version history
# MAGIC 1. Time travel to read previous version

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

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

# MAGIC %md **4.1: CHECK YOUR WORK**

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

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
