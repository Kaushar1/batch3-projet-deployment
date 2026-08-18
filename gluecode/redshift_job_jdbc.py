
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Config ---
connection_name = "batch3_demo_2907"     # <-- your existing Glue connection
redshift_schema = "sh1"                        # <-- schema name
redshift_table   = "cuatomer_account_list_2907"              # <-- table to read
redshift_tmp_dir = "s3://catalogdata1807/output/"   # scratch dir for unload
output_s3_path    = "s3://catalogdata1807/redshift_output/"  # final output location

try:
    # --- Read from Redshift table ---
    df = glueContext.create_data_frame.from_options(
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": connection_name,
            "dbtable": f"{redshift_schema}.{redshift_table}",
            "redshiftTmpDir": redshift_tmp_dir
        }
    )

    print("✅ Read from Redshift successful!")
    df.show(5)
    row_count = df.count()
    print("Row count fetched:", row_count)

    # --- Write to S3 (Parquet) ---
    df.write \
        .mode("overwrite") \
        .format("csv") \
        .save(output_s3_path)

    print(f"✅ Data written successfully to {output_s3_path}")

except Exception as e:
    print("❌ Connectivity/ETL test failed!")
    print(str(e))
    raise

job.commit()