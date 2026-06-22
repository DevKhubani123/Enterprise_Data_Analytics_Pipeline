# Databricks notebook source
# MAGIC %run ./01_customerdomain_config

# COMMAND ----------

dbutils.widgets.dropdown("batch","Batch1",["Batch1", "Batch2", "Batch3"])
batch = dbutils.widgets.get("batch")

# COMMAND ----------

watchhistory_landing_path=f"{vol_landing}/{batch}/WatchHistory"
df_bronze=spark.read.format('parquet').load(watchhistory_landing_path)
df_bronze.display()

# COMMAND ----------

df_bronze=df_bronze.drop("_landing_ts")
df_bronze = df_bronze.select(
    [col(c).cast("string").alias(c) for c in df_bronze.columns]
)
df_bronze = (
    df_bronze
    .withColumn("_ingest_ts", current_timestamp())
)
df_bronze.display()

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

df_bronze.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("_batch") \
    .saveAsTable(f"{bronze_sc}.watchhistory")