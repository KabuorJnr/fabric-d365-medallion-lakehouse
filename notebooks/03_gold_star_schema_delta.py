"""
Microsoft Fabric PySpark Notebook: 03_gold_star_schema_delta.py
Author: Rooney Odhiambo (Microsoft Certified: Fabric Data Engineer Associate, DP-700)
Purpose: Builds the curated Dimensional Star Schema in the Gold Lakehouse.
         Optimized for Power BI Direct Lake Mode via Delta Z-Ordering & Partitioning.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("D365_Silver_To_Gold_Dimensional_Model").getOrCreate()

# -----------------------------------------------------------------------------
# Lakehouse Path Configuration
# -----------------------------------------------------------------------------
SILVER_BASE = "abfss://ws_d365_analytics@onelake.dfs.fabric.microsoft.com/lh_d365_silver.Lakehouse/Tables"
GOLD_BASE = "abfss://ws_d365_analytics@onelake.dfs.fabric.microsoft.com/lh_d365_gold.Lakehouse/Tables"

# -----------------------------------------------------------------------------
# 1. Build Gold Dimension: DimDate (Calendar & Fiscal Mapping)
# -----------------------------------------------------------------------------
start_date = "2023-01-01"
end_date = "2028-12-31"

df_dates = spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as calendar_date
""")

df_dim_date = (df_dates
    .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("integer"))
    .withColumn("year", F.year("calendar_date"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("calendar_date")))
    .withColumn("month_number", F.month("calendar_date"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("month_short", F.date_format("calendar_date", "MMM"))
    .withColumn("day_of_month", F.dayofmonth("calendar_date"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("is_weekend", F.when(F.dayofweek("calendar_date").isin(1, 7), 1).otherwise(0))
    # East Africa Fiscal Year (July 1 - June 30)
    .withColumn("fiscal_year", F.when(F.month("calendar_date") >= 7, F.year("calendar_date") + 1).otherwise(F.year("calendar_date")))
    .withColumn("fiscal_quarter", F.when(F.month("calendar_date").between(7, 9), "FQ1")
                                 .when(F.month("calendar_date").between(10, 12), "FQ2")
                                 .when(F.month("calendar_date").between(1, 3), "FQ3")
                                 .otherwise("FQ4")))

(df_dim_date.write
    .format("delta")
    .mode("overwrite")
    .save(f"{GOLD_BASE}/DimDate"))

print("✅ DimDate built and written to Gold.")

# -----------------------------------------------------------------------------
# 2. Build Gold Dimension: DimCustomer (Conformed Dimension)
# -----------------------------------------------------------------------------
df_silver_cust = spark.read.format("delta").load(f"{SILVER_BASE}/dim_customer_silver")

df_gold_cust = (df_silver_cust
    .withColumn("customer_sk", F.abs(F.hash("customer_id")))
    .select(
        "customer_sk",
        "customer_id",
        "customer_account_number",
        "customer_name",
        "email",
        "phone",
        "city",
        "country",
        "credit_limit",
        "status_label",
        "modified_on_eat"
    ))

(df_gold_cust.write
    .format("delta")
    .mode("overwrite")
    .save(f"{GOLD_BASE}/DimCustomer"))

print("✅ DimCustomer built and written to Gold.")

# -----------------------------------------------------------------------------
# 3. Build Gold Fact: FactSales (Direct Lake Optimized)
# -----------------------------------------------------------------------------
df_silver_sales = spark.read.format("delta").load(f"{SILVER_BASE}/fct_sales_orders_silver")

df_fact_sales = (df_silver_sales
    .withColumn("date_key", F.date_format("order_date_eat", "yyyyMMdd").cast("integer"))
    .withColumn("customer_sk", F.abs(F.hash("customer_id")))
    .withColumn("net_amount", F.col("total_amount_gross") - F.col("discount_amount"))
    .withColumn("fiscal_year", F.when(F.month("order_date_eat") >= 7, F.year("order_date_eat") + 1).otherwise(F.year("order_date_eat")))
    .select(
        "order_id",
        "order_number",
        "date_key",
        "customer_sk",
        "total_amount_gross",
        "discount_amount",
        "net_amount",
        "tax_amount",
        "order_status_label",
        "fiscal_year"
    ))

# Partition by fiscal_year to facilitate pruning for Direct Lake Mode
(df_fact_sales.write
    .format("delta")
    .partitionBy("fiscal_year")
    .mode("overwrite")
    .save(f"{GOLD_BASE}/FactSales"))

print("✅ FactSales built, partitioned by fiscal_year, and written to Gold.")

# -----------------------------------------------------------------------------
# 4. Direct Lake Performance Optimization: Z-ORDER Indexing
# -----------------------------------------------------------------------------
# Z-Ordering co-locates related data in the same Delta Parquet files,
# enabling the Power BI VertiPaq engine to skip non-relevant row-groups on query execution.
spark.sql(f"""
    OPTIMIZE delta.`{GOLD_BASE}/FactSales`
    ZORDER BY (customer_sk, date_key)
""")

print("🚀 Z-Ordering complete on FactSales. Table is 100% primed for Power BI Direct Lake Mode.")
