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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-datalake-digx/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1683265008809 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-datalake-digx/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1683265008809",
)

# Script generated for node Renamed keys for ApplyMapping
RenamedkeysforApplyMapping_node1683527901986 = ApplyMapping.apply(
    frame=AmazonS3_node1683265008809,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"),
        ("serialNumber", "string", "serialnumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforApplyMapping_node1683527901986",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=RenamedkeysforApplyMapping_node1683527901986,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1683265087702 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "serialnumber",
    ],
    transformation_ctx="DropFields_node1683265087702",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1683265087702,
    database="stedi-datalake-digx",
    table_name="step_trainer_trusted",
    transformation_ctx="S3bucket_node3",
)

job.commit()
