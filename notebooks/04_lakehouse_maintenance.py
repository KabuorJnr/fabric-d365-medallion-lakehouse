"""
Microsoft Fabric PySpark Notebook: 04_lakehouse_maintenance.py
Author: Rooney Odhiambo (Microsoft Certified: Fabric Data Engineer Associate, DP-700)
Purpose: Weekly maintenance script for Microsoft Fabric Lakehouses:
         - OPTIMIZE (file compaction of small Parquet files)
         - VACUUM (removal of obsolete Delta snapshots beyond 7-day retention)
         - ANALYZE TABLE (compute statistics for the Fabric query engine)
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("D365_Lakehouse_Maintenance").getOrCreate()

LAKEHOUSE_GOLD = "abfss://ws_d365_analytics@onelake.dfs.fabric.microsoft.com/lh_d365_gold.Lakehouse/Tables"
GOLD_TABLES = ["FactSales", "DimCustomer", "DimDate"]

print("Starting Weekly Delta Lakehouse Maintenance Job...")

for table in GOLD_TABLES:
    table_path = f"{LAKEHOUSE_GOLD}/{table}"
    print(f"\n--- Processing Table: {table} ---")
    
    # 1. Compact small parquet files into optimal ~1GB chunks for Direct Lake
    print(f"Running OPTIMIZE on {table}...")
    spark.sql(f"OPTIMIZE delta.`{table_path}`")
    
    # 2. Retain 7 days of Delta time-travel snapshots, vacuum older obsolete versions
    print(f"Running VACUUM (retention: 168 hours) on {table}...")
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
    
    # 3. Compute statistics for query optimizer
    print(f"Computing column statistics for {table}...")
    spark.sql(f"ANALYZE TABLE delta.`{table_path}` COMPUTE STATISTICS FOR ALL COLUMNS")

print("\n✅ All Gold tables successfully optimized and compacted for Direct Lake queries.")
