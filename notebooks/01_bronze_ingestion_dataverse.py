"""
Microsoft Fabric PySpark Notebook: 01_bronze_ingestion_dataverse.py
Author: Rooney Odhiambo (Microsoft Certified: Fabric Data Engineer Associate, DP-700)
Purpose: Validate, audit, and log Dataverse / Dynamics 365 OneLake shortcuts in Bronze Lakehouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime

# Initialize Fabric Spark Session
spark = SparkSession.builder.appName("D365_Bronze_Ingestion_Audit").getOrCreate()

# -----------------------------------------------------------------------------
# Configuration & Lakehouse Paths
# -----------------------------------------------------------------------------
LAKEHOUSE_BRONZE = "abfss://ws_d365_analytics@onelake.dfs.fabric.microsoft.com/lh_d365_bronze.Lakehouse"
TABLES_TO_AUDIT = [
    "account",
    "contact",
    "salesorder",
    "salesorderdetail",
    "invoicedetail",
    "stringmap",
    "transactioncurrency"
]

print(f"[{datetime.now().isoformat()}] Starting Dataverse OneLake Bronze Ingestion Validation...")

# -----------------------------------------------------------------------------
# 1. Audit OneLake Shortcuts and Row Counts
# -----------------------------------------------------------------------------
audit_records = []

for table_name in TABLES_TO_AUDIT:
    try:
        # Read Delta table synced via Link to Microsoft Fabric
        df = spark.read.format("delta").load(f"{LAKEHOUSE_BRONZE}/Tables/{table_name}")
        row_count = df.count()
        column_count = len(df.columns)
        
        # Check for change tracking timestamp
        latest_timestamp = None
        if "modifiedon" in df.columns:
            latest_timestamp = df.select(F.max("modifiedon")).collect()[0][0]

        print(f"✅ Table '{table_name}': {row_count:,} rows, {column_count} columns. Latest modified: {latest_timestamp}")
        
        audit_records.append({
            "table_name": table_name,
            "status": "VALID",
            "row_count": row_count,
            "column_count": column_count,
            "latest_modifiedon": str(latest_timestamp),
            "audited_at": datetime.utcnow()
        })
    except Exception as e:
        print(f"❌ Error inspecting table '{table_name}': {str(e)}")
        audit_records.append({
            "table_name": table_name,
            "status": "ERROR",
            "row_count": 0,
            "column_count": 0,
            "latest_modifiedon": None,
            "audited_at": datetime.utcnow()
        })

# -----------------------------------------------------------------------------
# 2. Write Ingestion Audit Log into Bronze System Table
# -----------------------------------------------------------------------------
audit_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("row_count", IntegerType(), False),
    StructField("column_count", IntegerType(), False),
    StructField("latest_modifiedon", StringType(), True),
    StructField("audited_at", TimestampType(), False)
])

df_audit = spark.createDataFrame(audit_records, schema=audit_schema)

(df_audit.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(f"{LAKEHOUSE_BRONZE}/Tables/sys_ingestion_audit_log"))

print(f"[{datetime.now().isoformat()}] Bronze Ingestion Audit successfully logged to sys_ingestion_audit_log.")
