# Databricks notebook source
# MAGIC %run ./01_customerdomain_config

# COMMAND ----------

dbutils.widgets.dropdown("batch","Batch2",["Batch1", "Batch2", "Batch3"])
batch = dbutils.widgets.get("batch")

# COMMAND ----------

print(f"{batch}")

# COMMAND ----------

prospect_raw=f"{raw_base_path}/{batch}/prospect.json"

# COMMAND ----------

df_raw =spark.read.format("json").option("multiLine", True).load(prospect_raw)
df_exploded = df_raw.select(F.explode(F.col("prospect_batch.prospects")).alias("p"))
df_flat = df_exploded.select(
    F.col("p.agency_id").alias("agency_id"),
    F.col("p.personal.name.first_name").alias("first_name"),
    F.col("p.personal.name.last_name").alias("last_name"),
    F.col("p.personal.name.middle_initial").alias("middle_initial"),
    F.col("p.personal.demographics.gender").alias("gender"),
    F.col("p.personal.demographics.age").alias("age"),
    F.col("p.personal.demographics.marital_status").alias("marital_status"),
    F.col("p.contact.address.line1").alias("address_line1"),
    F.col("p.contact.address.line2").alias("address_line2"),
    F.col("p.contact.address.city").alias("city"),
    F.col("p.contact.address.state").alias("state"),
    F.col("p.contact.address.postal_code").alias("postal_code"),
    F.col("p.contact.address.country").alias("country"),
    F.col("p.contact.phone.full_number").alias("phone"),
    F.col("p.financial.income.annual_income").alias("income"),
    F.col("p.financial.wealth.net_worth").alias("net_worth"),
    F.col("p.financial.credit.credit_rating").alias("credit_rating"),
    F.col("p.financial.credit.number_credit_cards").alias("number_credit_cards"),
    F.col("p.lifestyle.housing.own_or_rent") .alias("own_or_rent_flag"),
    F.col("p.lifestyle.family.number_children").alias("number_children"),
    F.col("p.lifestyle.assets.number_cars").alias("number_cars"),
    F.col("p.employment.employer_name").alias("employer")
)

# COMMAND ----------

prosp_file=f"prospect.json"
df_landing = (
    df_flat
    .withColumn("_landing_ts", current_timestamp())
    .withColumn("_batch", lit(batch))
    .withColumn("_source_file", lit(prosp_file))
    .withColumn("_run_id", lit(RUN_ID))
)
df_landing.display()

# COMMAND ----------

prospect_landing=f"{vol_landing}/{batch}/prospect"
df_landing.write.format('parquet').mode('overwrite').save(prospect_landing)