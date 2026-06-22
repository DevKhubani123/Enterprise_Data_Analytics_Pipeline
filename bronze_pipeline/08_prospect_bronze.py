# Databricks notebook source
# MAGIC %run ./01_customerdomain_config

# COMMAND ----------

dbutils.widgets.dropdown("batch","Batch1",["Batch1", "Batch2", "Batch3"])
batch = dbutils.widgets.get("batch")

# COMMAND ----------

prospect_landing=f"{vol_landing}/{batch}/prospect"

# COMMAND ----------

# DBTITLE 1,Read prospect parquet data
# Check if parquet files exist before reading
files = [f for f in dbutils.fs.ls(prospect_landing) if f.name.endswith('.parquet')]
if not files:
    dbutils.notebook.exit(f"No parquet files found at {prospect_landing}. Skipping prospect bronze load.")

df_bronze = spark.read.format('parquet').load(prospect_landing)
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

# COMMAND ----------

df_bronze.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("_batch") \
    .saveAsTable(f"{bronze_sc}.prospect")