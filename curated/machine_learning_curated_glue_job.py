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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680893499412 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1680893499412",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680893667617 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1680893667617",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project",
    table_name="step_trainer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Rename Serial Number Field for Join
RenameSerialNumberFieldforJoin_node1680893587423 = RenameField.apply(
    frame=AWSGlueDataCatalog_node1680893499412,
    old_name="serialnumber",
    new_name="cc_serialnumber",
    transformation_ctx="RenameSerialNumberFieldforJoin_node1680893587423",
)

# Script generated for node Join step trainer and customer
Joinsteptrainerandcustomer_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=RenameSerialNumberFieldforJoin_node1680893587423,
    keys1=["serialnumber"],
    keys2=["cc_serialnumber"],
    transformation_ctx="Joinsteptrainerandcustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1680893555821 = DropFields.apply(
    frame=Joinsteptrainerandcustomer_node2,
    paths=[
        "customername",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "cc_serialnumber",
    ],
    transformation_ctx="DropFields_node1680893555821",
)

# Script generated for node Join
Join_node1680893699433 = Join.apply(
    frame1=AWSGlueDataCatalog_node1680893667617,
    frame2=DropFields_node1680893555821,
    keys1=["user", "timestamp"],
    keys2=["email", "sensorreadingtime"],
    transformation_ctx="Join_node1680893699433",
)

# Script generated for node Drop Fields
DropFields_node1680893792672 = DropFields.apply(
    frame=Join_node1680893699433,
    paths=["sensorreadingtime", "email"],
    transformation_ctx="DropFields_node1680893792672",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1680893792672,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://danwilentz-stedi-lake-house/project/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
