import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_landing
accelerometer_landing_node1769079147754 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-demo-bucket08/accelerometer/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1769079147754")

# Script generated for node customer_trusted
customer_trusted_node1769079146728 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-demo-bucket08/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1769079146728")

# Script generated for node Join
Join_node1769079172944 = Join.apply(frame1=accelerometer_landing_node1769079147754, frame2=customer_trusted_node1769079146728, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1769079172944")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct user,timestamp,x,y,z
FROM myDataSource

'''
SQLQuery_node1769079174431 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1769079172944}, transformation_ctx = "SQLQuery_node1769079174431")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769079174431, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769079126494", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1769079407654 = glueContext.getSink(path="s3://my-s3-demo-bucket08/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1769079407654")
accelerometer_trusted_node1769079407654.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1769079407654.setFormat("json")
accelerometer_trusted_node1769079407654.writeFrame(SQLQuery_node1769079174431)
job.commit()