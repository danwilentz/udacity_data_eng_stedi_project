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
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://danwilentz-stedi-lake-house/project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1680888959914 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1680888959914",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1680891962418 = DynamicFrame.fromDF(
    AmazonS3_node1680888959914.toDF().dropDuplicates(["serialnumber"]),
    glueContext,
    "DropDuplicates_node1680891962418",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1680889088711 = ApplyMapping.apply(
    frame=DropDuplicates_node1680891962418,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1680889088711",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=RenamedkeysforJoin_node1680889088711,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1680889119789 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1680889119789",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1680889119789,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danwilentz-stedi-lake-house/project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
