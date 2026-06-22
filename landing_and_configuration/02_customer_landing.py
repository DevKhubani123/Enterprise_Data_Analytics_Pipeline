# Databricks notebook source
# MAGIC %run ./01_customerdomain_config

# COMMAND ----------

dbutils.widgets.dropdown("batch","Batch1",["Batch1", "Batch2", "Batch3"])
batch = dbutils.widgets.get("batch")

# COMMAND ----------

customer_mgmt_path = f"{raw_base_path}/{batch}/CustomerMgmt.xml"
xml_file   = "CustomerMgmt.xml"
file=dbutils.fs.ls(customer_mgmt_path)
print(file)
print(dbutils.fs.head(customer_mgmt_path, 5000))


# COMMAND ----------

df = (
    spark.read
    .format("xml")
    .option("rowTag", "TPCDI:Action")
    .load(customer_mgmt_path)
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_flat = df.select(

    col("_ActionType").alias("ActionType"),
    col("_ActionTS").alias("ActionTS"),

    col("Customer._C_ID").alias("C_ID"),
    col("Customer._C_TAX_ID").alias("C_TAX_ID"),
    col("Customer._C_GNDR").alias("C_GNDR"),
    col("Customer._C_TIER").alias("C_TIER"),
    col("Customer._C_DOB").alias("C_DOB"),

    col("Customer.Name.C_L_NAME").alias("C_L_NAME"),
    col("Customer.Name.C_F_NAME").alias("C_F_NAME"),
    col("Customer.Name.C_M_NAME").alias("C_M_NAME"),

    col("Customer.Address.C_ADLINE1").alias("C_ADLINE1"),
    col("Customer.Address.C_ADLINE2").alias("C_ADLINE2"),
    col("Customer.Address.C_ZIPCODE").alias("C_ZIPCODE"),
    col("Customer.Address.C_CITY").alias("C_CITY"),
    col("Customer.Address.C_STATE_PROV").alias("C_STATE_PROV"),
    col("Customer.Address.C_CTRY").alias("C_CTRY"),

    col("Customer.ContactInfo.C_PRIM_EMAIL").alias("C_PRIM_EMAIL"),
    col("Customer.ContactInfo.C_ALT_EMAIL").alias("C_ALT_EMAIL"),

    col("Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE").alias("C_CTRY_CODE_1"),
    col("Customer.ContactInfo.C_PHONE_1.C_AREA_CODE").alias("C_AREA_CODE_1"),
    col("Customer.ContactInfo.C_PHONE_1.C_LOCAL").alias("C_LOCAL_1"),
    col("Customer.ContactInfo.C_PHONE_1.C_EXT").alias("C_EXT_1"),

    col("Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE").alias("C_CTRY_CODE_2"),
    col("Customer.ContactInfo.C_PHONE_2.C_AREA_CODE").alias("C_AREA_CODE_2"),
    col("Customer.ContactInfo.C_PHONE_2.C_LOCAL").alias("C_LOCAL_2"),
    col("Customer.ContactInfo.C_PHONE_2.C_EXT").alias("C_EXT_2"),

    col("Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE").alias("C_CTRY_CODE_3"),
    col("Customer.ContactInfo.C_PHONE_3.C_AREA_CODE").alias("C_AREA_CODE_3"),
    col("Customer.ContactInfo.C_PHONE_3.C_LOCAL").alias("C_LOCAL_3"),
    col("Customer.ContactInfo.C_PHONE_3.C_EXT").alias("C_EXT_3"),

    col("Customer.TaxInfo.C_LCL_TX_ID").alias("C_LCL_TX_ID"),
    col("Customer.TaxInfo.C_NAT_TX_ID").alias("C_NAT_TX_ID"),

    col("Customer.Account._CA_ID").alias("CA_ID"),
    col("Customer.Account._CA_TAX_ST").alias("CA_TAX_ST"),
    col("Customer.Account.CA_B_ID").alias("CA_B_ID"),
    col("Customer.Account.CA_NAME").alias("CA_NAME")
)

# COMMAND ----------

df_flat.display()

# COMMAND ----------

df_landing = (
    df_flat
    .withColumn("_landing_ts", current_timestamp())
    .withColumn("_batch", lit(batch))
    .withColumn("_source_file", lit(xml_file))
    .withColumn("_run_id", lit(RUN_ID))
)

# COMMAND ----------

df_landing.display()

# COMMAND ----------

print(f"{batch}")

# COMMAND ----------

xml_landing=f"{vol_landing}/{batch}/CustomerMgmt"
df_landing.write.mode("overwrite").format("parquet").save(xml_landing)

# COMMAND ----------

customer_txt_path = f"{raw_base_path}/Batch2/Customer.txt"
txt_file   = "Customer.txt"
print(dbutils.fs.head(customer_txt_path,10000))

# COMMAND ----------

df_cust1=spark.read.format("csv").option("header","false").option("delimiter","|").load(customer_txt_path)


# COMMAND ----------

df_cust1.display()

# COMMAND ----------

customer_columns = [
        "CDC_FLAG", "CDC_DSN",
        "C_ID", "C_TAX_ID", "C_ST_ID", "C_L_NAME", "C_F_NAME", "C_M_NAME",
        "C_GNDR", "C_TIER", "C_DOB", "C_ADLINE1", "C_ADLINE2", "C_ZIPCODE",
        "C_CITY", "C_STATE_PROV", "C_CTRY",
        "C_CTRY_CODE_1", "C_AREA_CODE_1", "C_LOCAL_1", "C_EXT_1",
        "C_CTRY_CODE_2", "C_AREA_CODE_2", "C_LOCAL_2", "C_EXT_2",
        "C_CTRY_CODE_3", "C_AREA_CODE_3", "C_LOCAL_3", "C_EXT_3",
        "C_PRIM_EMAIL", "C_ALT_EMAIL",
        "C_LCL_TX_ID", "C_NAT_TX_ID"
]
df_cust1=df_cust1.toDF(*customer_columns)
df_cust1.display()

# COMMAND ----------

df_cust1_landing=(
    df_cust1
    .withColumn("_landing_ts", current_timestamp())
    .withColumn("_batch", lit(batch))
    .withColumn("_source_file", lit(txt_file))
    .withColumn("_run_id", lit(RUN_ID))
)

# COMMAND ----------

df_cust1_landing.display()

# COMMAND ----------

df_cust1_landing.count()

# COMMAND ----------

print(f"{batch}")

# COMMAND ----------

customer1_txt_landing=f"{vol_landing}/{batch}/Customer"
df_cust1_landing.write.mode("overwrite").format("parquet").save(customer1_txt_landing)

# COMMAND ----------

print(f"{batch}")

# COMMAND ----------

customer_txt2_path = f"{raw_base_path}/Batch3/Customer.txt"
txt_file   = "Customer.txt"

# COMMAND ----------

df_cust2=spark.read.format("csv").option("header","false").option("delimiter","|").load(customer_txt2_path)

# COMMAND ----------

customer_columns = [
        "CDC_FLAG", "CDC_DSN",
        "C_ID", "C_TAX_ID", "C_ST_ID", "C_L_NAME", "C_F_NAME", "C_M_NAME",
        "C_GNDR", "C_TIER", "C_DOB", "C_ADLINE1", "C_ADLINE2", "C_ZIPCODE",
        "C_CITY", "C_STATE_PROV", "C_CTRY",
        "C_CTRY_CODE_1", "C_AREA_CODE_1", "C_LOCAL_1", "C_EXT_1",
        "C_CTRY_CODE_2", "C_AREA_CODE_2", "C_LOCAL_2", "C_EXT_2",
        "C_CTRY_CODE_3", "C_AREA_CODE_3", "C_LOCAL_3", "C_EXT_3",
        "C_PRIM_EMAIL", "C_ALT_EMAIL",
        "C_LCL_TX_ID", "C_NAT_TX_ID"
]
df_cust2=df_cust2.toDF(*customer_columns)
df_cust2.display()

# COMMAND ----------

df_cust2_landing=(
    df_cust2
    .withColumn("_landing_ts", current_timestamp())
    .withColumn("_batch", lit(batch))
    .withColumn("_source_file", lit(txt_file))
    .withColumn("_run_id", lit(RUN_ID))
)
df_cust2_landing.display()

# COMMAND ----------

customer2_txt_landing=f"{vol_landing}/{batch}/Customer"
df_cust2_landing.write.mode("overwrite").format("parquet").save(customer2_txt_landing)