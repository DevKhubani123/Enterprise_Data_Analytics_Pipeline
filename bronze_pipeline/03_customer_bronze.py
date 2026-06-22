# Databricks notebook source
# MAGIC %run "./01_customerdomain_config"

# COMMAND ----------

dbutils.widgets.dropdown("batch","Batch1",["Batch1", "Batch2", "Batch3"])
batch = dbutils.widgets.get("batch")

# COMMAND ----------

xml_landing=f"{vol_landing}/{batch}/CustomerMgmt"
df_custmgmt_bronze=spark.read.format('parquet').load(xml_landing)
df_custmgmt_bronze.display()

# COMMAND ----------

df_bronze=df_custmgmt_bronze.drop("_landing_ts")

# COMMAND ----------

df_bronze = df_bronze.select(
    [col(c).cast("string").alias(c) for c in df_bronze.columns]
)
df_bronze = (
    df_bronze
    .withColumn("_ingest_ts", current_timestamp())
)

# COMMAND ----------

df_bronze.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("_batch") \
    .saveAsTable(f"{bronze_sc}.customermgmt")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM charles_schwab_retailbrokerage_dev_team_zeta.bronze.customermgmt;

# COMMAND ----------

#customer txt batch 2 and batch 3
customer1_txt_landing=f"{vol_landing}/Batch2/Customer"
txt_file   = "Customer.txt"

# COMMAND ----------

df_cust1=spark.read.format('parquet').load(customer1_txt_landing)
df_cust1.display()

# COMMAND ----------

df_cust1_bronze=df_cust1.drop("_landing_ts")

# COMMAND ----------

df_cust1_bronze = df_cust1_bronze.select(
    [col(c).cast("string").alias(c) for c in df_cust1_bronze.columns]
)
df_cust1_bronze = (
    df_cust1_bronze
    .withColumn("_ingest_ts", current_timestamp())
)

# COMMAND ----------

df_cust1_bronze.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("_batch") \
    .saveAsTable(f"{bronze_sc}.customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM charles_schwab_retailbrokerage_dev_team_zeta.bronze.customer;

# COMMAND ----------

customer2_txt_landing=f"{vol_landing}/Batch3/Customer"
df_cust2=spark.read.format('parquet').load(customer2_txt_landing)
df_cust2.display()

# COMMAND ----------

df_cust2_bronze=df_cust2.drop("_landing_ts")
df_cust2_bronze = df_cust2_bronze.select(
    [col(c).cast("string").alias(c) for c in df_cust2_bronze.columns]
)
df_cust2_bronze = (
    df_cust2_bronze
    .withColumn("_ingest_ts", current_timestamp())
)

# COMMAND ----------

df_cust2_bronze.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("_batch") \
    .saveAsTable(f"{bronze_sc}.customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM charles_schwab_retailbrokerage_dev_team_zeta.bronze.customer;