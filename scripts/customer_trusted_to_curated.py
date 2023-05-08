import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-datalake-digx",
    table_name="accelerometer_landing_project",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1683262308514 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-datalake-digx",
    table_name="customer_trusted_project",
    transformation_ctx="CustomerTrusted_node1683262308514",
)

# Script generated for node Join Customer with Accelerometer
JoinCustomerwithAccelerometer_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1683262308514,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerwithAccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1683262378171 = DropFields.apply(
    frame=JoinCustomerwithAccelerometer_node2,
    paths=["x", "y", "user", "z"],
    transformation_ctx="DropFields_node1683262378171",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1683262378171,
    database="stedi-datalake-digx",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
