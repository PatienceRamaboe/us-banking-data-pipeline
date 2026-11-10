
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# --------------------------------------------------
# 1. Initialise the AWS Glue job
# --------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# 2. Define S3 source and destination paths
# --------------------------------------------------

transaction_path = (
    "s3://american-trust-bank-data-lake-2026/"
    "raw/ieee_cis/train_transaction.csv"
)

identity_path = (
    "s3://american-trust-bank-data-lake-2026/"
    "raw/ieee_cis/train_identity.csv"
)

output_path = (
    "s3://american-trust-bank-data-lake-2026/"
    "processed/ieee_cis/"
)

# --------------------------------------------------
# 3. Extract: Read the CSV datasets
# --------------------------------------------------

print("Reading transaction dataset...")

transactions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(transaction_path)
)

print("Reading identity dataset...")

identity = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(identity_path)
)

# --------------------------------------------------
# 4. Transform: Prepare the join key
# --------------------------------------------------

transactions = transactions.withColumn(
    "TransactionID",
    col("TransactionID").cast("long")
)

identity = identity.withColumn(
    "TransactionID",
    col("TransactionID").cast("long")
)

# --------------------------------------------------
# 5. Transform: Join both datasets
# --------------------------------------------------

print("Joining transaction and identity datasets...")

joined_data = transactions.join(
    identity,
    on="TransactionID",
    how="left"
)

# --------------------------------------------------
# 6. Transform: Convert important numeric columns
# --------------------------------------------------

joined_data = (
    joined_data
    .withColumn("TransactionAmt", col("TransactionAmt").cast("double"))
    .withColumn("TransactionDT", col("TransactionDT").cast("long"))
    .withColumn("isFraud", col("isFraud").cast("integer"))
)

# --------------------------------------------------
# 7. Load: Write processed data as Parquet
# --------------------------------------------------

print("Writing processed data to S3...")

(
    joined_data.write
    .mode("overwrite")
    .parquet(output_path)
)

print("ETL processing completed successfully.")

# --------------------------------------------------
# 8. Complete the Glue job
# --------------------------------------------------

job.commit()
