import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Movies_data_S3
Movies_data_S3_node1739918156033 = glueContext.create_dynamic_frame.from_catalog(database="movies_db", table_name="moviesinput", transformation_ctx="Movies_data_S3_node1739918156033")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1739918197254_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        RowCount > 0,
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

EvaluateDataQuality_node1739918197254 = EvaluateDataQuality().process_rows(frame=Movies_data_S3_node1739918156033, ruleset=EvaluateDataQuality_node1739918197254_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739918197254", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1739918376333 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1739918197254, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1739918376333")

# Script generated for node ruleOutcomes
ruleOutcomes_node1739918500950 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1739918197254, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1739918500950")

# Script generated for node Conditional Router
ConditionalRouter_node1739918774287 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1739918376333,
  group_filters = [GroupFilter(name = "output_group_1", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node output_group_1
output_group_1_node1739918774918 = SelectFromCollection.apply(dfc=ConditionalRouter_node1739918774287, key="output_group_1", transformation_ctx="output_group_1_node1739918774918")

# Script generated for node default_group
default_group_node1739918774796 = SelectFromCollection.apply(dfc=ConditionalRouter_node1739918774287, key="default_group", transformation_ctx="default_group_node1739918774796")

# Script generated for node Change Schema
ChangeSchema_node1739918885002 = ApplyMapping.apply(frame=default_group_node1739918774796, mappings=[("poster_link", "string", "poster_link", "string"), ("series_title", "string", "series_title", "string"), ("released_year", "string", "released_year", "string"), ("certificate", "string", "certificate", "string"), ("runtime", "string", "runtime", "string"), ("genre", "string", "genre", "string"), ("imdb_rating", "double", "imdb_rating", "double"), ("overview", "string", "overview", "string"), ("meta_score", "long", "meta_score", "long"), ("director", "string", "director", "string"), ("star1", "string", "star1", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("star4", "string", "star4", "string"), ("no_of_votes", "long", "no_of_votes", "long"), ("gross", "string", "gross", "string"), ("DataQualityRulesPass", "array", "DataQualityRulesPass", "array"), ("DataQualityRulesFail", "array", "DataQualityRulesFail", "array"), ("DataQualityRulesSkip", "array", "DataQualityRulesSkip", "array"), ("DataQualityEvaluationResult", "string", "DataQualityEvaluationResult", "string")], transformation_ctx="ChangeSchema_node1739918885002")

# Script generated for node Amazon S3
AmazonS3_node1739918643379 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1739918500950, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-gds/rules_outcomes/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1739918643379")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1739919989843 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1739918885002, database="movies_db", table_name="redshift_dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://airlines-temp-storage/write/",transformation_ctx="AWSGlueDataCatalog_node1739919989843")

job.commit()