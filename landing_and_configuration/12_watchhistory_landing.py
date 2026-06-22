# Databricks notebook source
# MAGIC %run ./01_customerdomain_config

# COMMAND ----------

dbutils.widgets.dropdown("batch","Batch1",["Batch1", "Batch2", "Batch3"])
batch = dbutils.widgets.get("batch")

# COMMAND ----------

watch_history_source=source_path = f"{raw_base_path}/{batch}/WatchHistory.txt"

# COMMAND ----------

file=dbutils.fs.ls(watch_history_source)
print(file)

# COMMAND ----------

df=spark.read.format("csv").option("header", "false").option("delimiter", "|").load(watch_history_source)
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

if batch == "Batch1":

    watchhistory_df = (
        df.toDF(
            "W_C_ID",
            "W_S_SYMB",
            "W_DTS",
            "W_ACTION"
        )
        .withColumn("CDC_FLAG", F.lit("I"))
        .withColumn(
            "CDC_DSN",
            F.lit(None).cast("string")
        )
        )
else:

    watchhistory_df = df.toDF(
        "CDC_FLAG",
        "CDC_DSN",
        "W_C_ID",
        "W_S_SYMB",
        "W_DTS",
        "W_ACTION"
    )
watchhistory_df = watchhistory_df.select(
        "CDC_FLAG",
        "CDC_DSN",
        "W_C_ID",
        "W_S_SYMB",
        "W_DTS",
        "W_ACTION"
    )

# COMMAND ----------

watch_history_file=f"WatchHistory.txt"
watchistory_landing = (
    watchhistory_df
    .withColumn("_landing_ts", current_timestamp())
    .withColumn("_batch", lit(batch))
    .withColumn("_source_file", lit(watch_history_file))
    .withColumn("_run_id", lit(RUN_ID))
)
watchistory_landing.display()

# COMMAND ----------

watchhistory_landing_path=f"{vol_landing}/{batch}/WatchHistory"
watchistory_landing.write.mode('overwrite').format('parquet').save(watchhistory_landing_path)