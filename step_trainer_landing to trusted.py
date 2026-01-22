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

# Script generated for node customer_curated
customer_curated_node1769082750885 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-demo-bucket08/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1769082750885")

# Script generated for node step_trainer_landing
step_trainer_landing_node1769082751164 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-demo-bucket08/step_trainer/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1769082751164")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.serialnumber,s.sensorreadingtime, s.distancefromobject
from s join c on s.serialnumber=c.serialnumber

'''
SQLQuery_node1769082757820 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":step_trainer_landing_node1769082751164, "c":customer_curated_node1769082750885}, transformation_ctx = "SQLQuery_node1769082757820")

# Script generated for node SQL Query
SqlQuery1 = '''
select distinct serialnumber, sensorreadingtime, distancefromobject
From myDataSource
'''
SQLQuery_node1769082758234 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":SQLQuery_node1769082757820}, transformation_ctx = "SQLQuery_node1769082758234")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769082758234, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769082697796", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1769082768677 = glueContext.getSink(path="s3://my-s3-demo-bucket08/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1769082768677")
step_trainer_trusted_node1769082768677.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1769082768677.setFormat("json")
step_trainer_trusted_node1769082768677.writeFrame(SQLQuery_node1769082758234)
job.commit()