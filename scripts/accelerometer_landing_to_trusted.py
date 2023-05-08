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

# Script generated for node Customer Trusted
CustomerTrusted_node1683260936443 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-datalake-digx/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1683260936443",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-datalake-digx/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Join Customer with Accelerometer
JoinCustomerwithAccelerometer_node2 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=CustomerTrusted_node1683260936443,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerwithAccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1683261269164 = DropFields.apply(
    frame=JoinCustomerwithAccelerometer_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1683261269164",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1683261269164,
    database="stedi-datalake-digx",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
