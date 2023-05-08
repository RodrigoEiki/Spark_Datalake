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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-datalake-digx",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1683527349827 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-datalake-digx",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1683527349827",
)

# Script generated for node Join Step Trainer with Accelerometer
JoinStepTrainerwithAccelerometer_node2 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1683527349827,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="JoinStepTrainerwithAccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1683527507377 = DropFields.apply(
    frame=JoinStepTrainerwithAccelerometer_node2,
    paths=["user"],
    transformation_ctx="DropFields_node1683527507377",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1683527507377,
    database="stedi-datalake-digx",
    table_name="step_trainer_curated",
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
