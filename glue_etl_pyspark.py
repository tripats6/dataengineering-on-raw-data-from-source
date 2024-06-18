import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

predicate_pushdown = "region in ('ca','gb','us')"

# Script generated for node Amazon S3
AmazonS3_node1718711788126 = glueContext.create_dynamic_frame.from_catalog(database="de_yt_raw", table_name="raw_statistics", transformation_ctx="AmazonS3_node1718711788126", push_down_predicate = predicate_pushdown)

# Script generated for node Change Schema
ChangeSchema_node1718711915372 = ApplyMapping.apply(frame=AmazonS3_node1718711788126, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "string"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "string"), ("likes", "long", "likes", "string"), ("dislikes", "long", "dislikes", "string"), ("comment_count", "long", "comment_count", "string"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "string"), ("ratings_disabled", "boolean", "ratings_disabled", "string"), ("video_error_or_removed", "boolean", "video_error_or_removed", "string"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1718711915372")

# Script generated for node Amazon S3
AmazonS3_node1718720781075 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1718711915372, connection_type="s3", format="glueparquet", connection_options={"path": "s3://de-on-yt-data-cleaned-dev/youtube/raw_statistics/", "partitionKeys": ["region"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1718720781075")

job.commit()
