import dlt
import geopy  
import pandas as pd
from pyspark.sql.functions import col, avg
from typing import Iterator
import random

try:
    from transformations.utilities.libraries import get_lat_long
except ImportError:
    from utilities.libraries import get_lat_long

# Configuration
catalog = "insurance_claim_project"
silver_schema = "silver"
gold_schema = "gold"

#  2. AGGREGATED TELEMATICS (GOLD)
@dlt.table(
    # FIX: Added catalog and gold_schema to the table name
    name=f"{catalog}.{gold_schema}.gold_aggregated_telematics",
    comment="Average telematics data per vehicle",
    table_properties={"quality": "gold"}
)
def gold_aggregated_telematics():
    return (
        dlt.read(f"{catalog}.{silver_schema}.telematics")
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("telematics_speed"),
            avg("latitude").alias("telematics_latitude"),
            avg("longitude").alias("telematics_longitude"),
        )
    )

# --- 3. CUSTOMER CLAIM POLICY (GOLD) ---
@dlt.table(
    # FIX: Added catalog and gold_schema to the table name
    name=f"{catalog}.{gold_schema}.customer_claim_policy",
    comment="Joined table of claims, policies, and customers",
    table_properties={"quality": "gold"}
)
def gold_customer_claim_policy():
    policy = dlt.read(f"{catalog}.{silver_schema}.policy")
    claim = dlt.read(f"{catalog}.{silver_schema}.claim")
    customer = dlt.read(f"{catalog}.{silver_schema}.customer") 

    claim_policy = claim.join(policy, "policy_no")
    return claim_policy.join(customer, claim_policy.CUST_ID == customer.customer_id)

# --- 4. FINAL ENRICHED TABLE (GOLD) ---
@dlt.table(
    # FIX: Added catalog and gold_schema to the table name
    name=f"{catalog}.{gold_schema}.customer_claim_policy_telematics",
    comment="Final enriched dataset with geocoding and telematics",
    table_properties={"quality": "gold"}
)
def gold_final_enriched_table():
    # Use the fully qualified name to read the gold table defined above
    telematics_df = (
        dlt.read(f"{catalog}.{gold_schema}.gold_aggregated_telematics")
        .withColumnRenamed("chassis_no", "CHASSIS_NO")
    )
    
    # Use the fully qualified name to read the gold table defined above
    main_df = dlt.read(f"{catalog}.{gold_schema}.customer_claim_policy").where("address IS NOT NULL")
    
    return (
        main_df
        .withColumn("lat_long", get_lat_long(col("address")))
        .withColumn("latitude", col("lat_long.latitude"))
        .withColumn("longitude", col("lat_long.longitude"))
        .join(telematics_df, "CHASSIS_NO", how="left")
        .drop("lat_long")
    )
