from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap,
    split, size, when, concat, lit, abs, to_timestamp, regexp_extract
)
import dlt #delta live tables 

# Configuration of the schema 
catalog = "insurance_claim_project"
bronze_schema = "bronze"
silver_schema = "silver"

# --- 1. CLEAN TELEMATICS ---
@dlt.table(
    name="telematics",
    comment="cleaned telematics event",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_coordinates", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")
def telematics():
    # Source: bronze_telematics
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.bronze_telematics")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .drop("_rescued_data")
    )

# --- 2. CLEAN POLICY ---
@dlt.table(
    name="policy",
    comment="Cleaned policies",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_policy_no", "policy_no IS NOT NULL")
def policy():
    # Source: policies
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.policies")
        .withColumn("premium", abs("premium"))
        .drop("_rescued_data")
    )

# --- 3. CLEAN CLAIM ---
@dlt.table(
    name="claim",
    comment="Cleaned claims",
    table_properties={"quality": "silver"}
)
# Fix: Expectation now points to the renamed column 'incident_hour'
@dlt.expect_all({
    "valid_claim_number": "claim_no IS NOT NULL",
    "valid_incident_hour": "incident_hour BETWEEN 0 AND 23"
})
def claim():
    # Source: claims
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claims")
    return (
        df
        # Mapping raw 'date' -> 'incident_date'
        .withColumn("incident_date", col("date"))
        # Mapping raw 'hour' -> 'incident_hour'
        .withColumn("incident_hour", col("hour").cast("int"))
        # Keep existing columns that are already formatted as dates
        .withColumn("claim_date", col("claim_date"))
        .withColumn("license_issue_date", col("license_issue_date"))
        # Clean up by dropping the original raw names
        .drop("date", "hour", "_rescued_data")
    )

# --- 4. CLEAN CUSTOMER ---
@dlt.table(
    name="customer",
    comment="Cleaned customers",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL"
})
def customer():
    # Source: customers
    df = dlt.readStream(f"{catalog}.{bronze_schema}.customers")

    name_normalized = when(
        size(split(trim(col("name")), ",")) == 2,
        concat(
            initcap(trim(split(col("name"), ",").getItem(1))), lit(" "),
            initcap(trim(split(col("name"), ",").getItem(0)))
        )
    ).otherwise(initcap(trim(col("name"))))

    return (
        df
        .withColumn("date_of_birth", to_date(col("date_of_birth"), "dd-MM-yyyy"))
        .withColumn("firstname", split(name_normalized, " ").getItem(0))
        .withColumn("lastname", split(name_normalized, " ").getItem(1))
        .withColumn("address", concat(col("BOROUGH"), lit(", "), col("ZIP_CODE")))
        .drop("name", "BOROUGH", "ZIP_CODE", "_rescued_data")
    )

# --- 5. CLEAN TRAINING IMAGES ---
@dlt.table(
    name="training_images",
    comment="Enriched accident training image",
    table_properties={"quality": "silver"}
)
def training_images():
    # Source: training_images
    df = dlt.readStream(f"{catalog}.{bronze_schema}.training_images")
    return df.withColumn(
        "label",
        regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2)
    )

# --- 6. CLEAN CLAIM IMAGES ---
@dlt.table(
    name="claim_images",
    comment="Enriched claim image",
    table_properties={"quality": "silver"}
)
def claim_images():
    # Source: claim_images
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim_images")
    return df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))
