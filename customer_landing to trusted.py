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

# Script generated for node customer_landing1
customer_landing1_node1769078340238 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-s3-demo-bucket08/customer/"], "recurse": True}, transformation_ctx="customer_landing1_node1769078340238")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;

'''
SQLQuery_node1769078343654 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_landing1_node1769078340238}, transformation_ctx = "SQLQuery_node1769078343654")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769078343654, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769078112119", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1769078346698 = glueContext.getSink(path="s3://my-s3-demo-bucket08/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1769078346698")
customer_trusted_node1769078346698.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1769078346698.setFormat("json")
customer_trusted_node1769078346698.writeFrame(SQLQuery_node1769078343654)
job.commit()