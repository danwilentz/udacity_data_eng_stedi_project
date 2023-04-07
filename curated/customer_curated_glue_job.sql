import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="accelerometer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1680810827632 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1680810827632",
)

# Script generated for node Select Fields
SelectFields_node1680811820235 = SelectFields.apply(
    frame=S3bucket_node1,
    paths=["user"],
    transformation_ctx="SelectFields_node1680811820235",
)

# Script generated for node Grab distinct accelerometer users
Grabdistinctaccelerometerusers_node2 = DynamicFrame.fromDF(
    SelectFields_node1680811820235.toDF().dropDuplicates(),
    glueContext,
    "Grabdistinctaccelerometerusers_node2",
)

# Script generated for node Join
Join_node1680810814942 = Join.apply(
    frame1=Grabdistinctaccelerometerusers_node2,
    frame2=AmazonS3_node1680810827632,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1680810814942",
)

# Script generated for node Drop Fields
DropFields_node1680810936715 = DropFields.apply(
    frame=Join_node1680810814942,
    paths=[],
    transformation_ctx="DropFields_node1680810936715",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1680810936715,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danwilentz-stedi-lake-house/project/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
