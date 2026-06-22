# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone
from pyspark.sql.functions import *
from pyspark.sql.window import Window

dbutils.widgets.dropdown("batch","Batch1",["Batch1", "Batch2", "Batch3"])
batch_id = dbutils.widgets.get("batch")

raw_base_path = "abfss://raw@schwabdldevsa.dfs.core.windows.net"
landing_base_path = "abfss://landing@schwabdldevsa.dfs.core.windows.net"

catalog = "charles_schwab_retailbrokerage_dev_team_zeta"
landing_sc = f"{catalog}.landing"
vol_landing=f"/Volumes/{catalog}/landing/landing_team_zeta"
bronze_sc = f"{catalog}.bronze"
silver_sc = f"{catalog}.silver"
gold_sc = f"{catalog}.gold"
operations_sc = f"{catalog}.operations"
staging_sc=f"{catalog}.staging"

RUN_ID  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

#Helper Function to add metadata/audit columns in the table where required
def add_metadata(df, batch, file_name):

    return (
        df
        .withColumn("_batch", F.lit(batch))
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_landing_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(RUN_ID))
    )

# COMMAND ----------

