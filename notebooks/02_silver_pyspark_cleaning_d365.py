"""
Microsoft Fabric PySpark Notebook: 02_silver_pyspark_cleaning_d365.py
Author: Rooney Odhiambo (Microsoft Certified: Fabric Data Engineer Associate, DP-700)
Purpose: Cleans, decodes option sets, normalizes timezones, and deduplicates Dynamics 365
         tables from Bronze to Silver Lakehouse using Delta Lake MERGE (Upsert).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("D365_Bronze_To_Silver_Cleansing").getOrCreate()

# -----------------------------------------------------------------------------
# Lakehouse Path Configuration
# -----------------------------------------------------------------------------
BRONZE_BASE = "abfss://ws_d365_analytics@onelake.dfs.fabric.microsoft.com/lh_d365_bronze.Lakehouse/Tables"
SILVER_BASE = "abfss://ws_d365_analytics@onelake.dfs.fabric.microsoft.com/lh_d365_silver.Lakehouse/Tables"

# -----------------------------------------------------------------------------
# 1. Load Reference Metadata: Dataverse StringMap (Option-Set Choice Labels)
# -----------------------------------------------------------------------------
# Dataverse stores statuses/choice options as raw integers. StringMap maps these to human labels.
df_stringmap = spark.read.format("delta").load(f"{BRONZE_BASE}/stringmap")

def decode_optionset(df_source, entity_name, attribute_name, output_col_name):
    """Broadcast joins StringMap to resolve integer option-sets into human labels."""
    df_labels = (df_stringmap
        .filter((F.col("objecttypecode") == entity_name) & (F.col("attributename") == attribute_name))
        .select(
            F.col("attributevalue").cast("integer").alias("ref_code"),
            F.col("value").alias(output_col_name)
        ))
    
    return df_source.join(
        F.broadcast(df_labels),
        df_source[attribute_name] == df_labels["ref_code"],
        "left"
    ).drop("ref_code")

# -----------------------------------------------------------------------------
# 2. Process & Clean: Account (Customer) Entity
# -----------------------------------------------------------------------------
df_raw_account = spark.read.format("delta").load(f"{BRONZE_BASE}/account")

# Deduplicate based on modifiedon timestamp
window_spec = Window.partitionBy("accountid").orderBy(F.col("modifiedon").desc())

df_clean_account = (df_raw_account
    .withColumn("row_rank", F.row_number().over(window_spec))
    .filter(F.col("row_rank") == 1)
    .drop("row_rank")
    # Standardize timezones from UTC to East Africa Time (EAT / Africa/Nairobi)
    .withColumn("created_on_eat", F.from_utc_timestamp(F.col("createdon"), "Africa/Nairobi"))
    .withColumn("modified_on_eat", F.from_utc_timestamp(F.col("modifiedon"), "Africa/Nairobi"))
    # Standardize phone numbers and email
    .withColumn("telephone1_cleaned", F.regexp_replace(F.col("telephone1"), "[^0-9+]", ""))
    .withColumn("emailaddress1_cleaned", F.lower(F.trim(F.col("emailaddress1"))))
    .select(
        F.col("accountid").alias("customer_id"),
        F.col("accountnumber").alias("customer_account_number"),
        F.col("name").alias("customer_name"),
        F.col("emailaddress1_cleaned").alias("email"),
        F.col("telephone1_cleaned").alias("phone"),
        F.col("address1_city").alias("city"),
        F.col("address1_country").alias("country"),
        F.col("creditlimit").cast("double").alias("credit_limit"),
        F.col("statecode").alias("state_code"),
        F.col("statuscode").alias("status_code"),
        F.col("created_on_eat"),
        F.col("modified_on_eat"),
        F.current_timestamp().alias("silver_ingested_at")
    ))

# Resolve state and status labels
df_clean_account = decode_optionset(df_clean_account, "account", "status_code", "status_label")

# Upsert (MERGE) into Silver Lakehouse Table
silver_account_path = f"{SILVER_BASE}/dim_customer_silver"
if DeltaTable.isDeltaTable(spark, silver_account_path):
    target_table = DeltaTable.forPath(spark, silver_account_path)
    (target_table.alias("target")
        .merge(
            df_clean_account.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    (df_clean_account.write
        .format("delta")
        .mode("overwrite")
        .save(silver_account_path))

print("✅ Successfully cleansed & merged Customer entity into Silver.")

# -----------------------------------------------------------------------------
# 3. Process & Clean: Sales Order / Invoices Entity
# -----------------------------------------------------------------------------
df_raw_orders = spark.read.format("delta").load(f"{BRONZE_BASE}/salesorder")

df_clean_orders = (df_raw_orders
    .withColumn("row_rank", F.row_number().over(Window.partitionBy("salesorderid").orderBy(F.col("modifiedon").desc())))
    .filter(F.col("row_rank") == 1)
    .drop("row_rank")
    .withColumn("order_date_eat", F.from_utc_timestamp(F.col("createdon"), "Africa/Nairobi"))
    .select(
        F.col("salesorderid").alias("order_id"),
        F.col("ordernumber").alias("order_number"),
        F.col("customerid").alias("customer_id"),
        F.col("totalamount").cast("double").alias("total_amount_gross"),
        F.col("totaltax").cast("double").alias("tax_amount"),
        F.col("totaldiscountamount").cast("double").alias("discount_amount"),
        F.col("statuscode").alias("order_status_code"),
        F.col("transactioncurrencyid").alias("currency_id"),
        F.col("order_date_eat"),
        F.current_timestamp().alias("silver_ingested_at")
    ))

df_clean_orders = decode_optionset(df_clean_orders, "salesorder", "order_status_code", "order_status_label")

silver_orders_path = f"{SILVER_BASE}/fct_sales_orders_silver"
if DeltaTable.isDeltaTable(spark, silver_orders_path):
    target_table = DeltaTable.forPath(spark, silver_orders_path)
    (target_table.alias("target")
        .merge(
            df_clean_orders.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    (df_clean_orders.write
        .format("delta")
        .mode("overwrite")
        .save(silver_orders_path))

print("✅ Successfully cleansed & merged Sales Orders entity into Silver.")
