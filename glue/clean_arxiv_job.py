"""Glue job to clean arxiv data from raw bucket json to cleaned bucket parquet."""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from pyspark.sql.functions import date_format, substring
from pyspark.sql import Window
from pyspark.sql.types import *
import requests
import pymupdf
import re


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(
        "(select * from source1) UNION " + unionType + " (select * from source2)"
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog rawbiorxiv
AWSGlueDataCatalograwbiorxiv_node1731561226347 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="rawarxivdb",
        table_name="rawbiorxiv",
        transformation_ctx="AWSGlueDataCatalograwbiorxiv_node1731561226347",
    )
)

# Script generated for node AWS Glue Data Catalog rawmedrxiv
AWSGlueDataCatalograwmedrxiv_node1731561276733 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="rawarxivdb",
        table_name="rawmedrxiv",
        transformation_ctx="AWSGlueDataCatalograwmedrxiv_node1731561276733",
    )
)

# Script generated for node Union
Union_node1731561525525 = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={
        "source1": AWSGlueDataCatalograwmedrxiv_node1731561276733,
        "source2": AWSGlueDataCatalograwbiorxiv_node1731561226347,
    },
    transformation_ctx="Union_node1731561525525",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1731561816247 = Union_node1731561525525.gs_null_rows()

# Drop duplicates of doi and only keep latest version of each doi
w = Window.partitionBy("doi").orderBy(SqlFuncs.desc("version"))

# node Drop Duplicates
DropDuplicates_node1731562207467 = DynamicFrame.fromDF(
    RemoveNullRows_node1731561816247.toDF()
    .withColumn("rn", SqlFuncs.row_number().over(w))
    .filter("rn = 1")
    .drop("rn"),
    glueContext,
    "DropDuplicates_node1731562207467",
)

# drop fields version, type, license, jatsxml
DropFields_node = DropFields.apply(
    frame=DropDuplicates_node1731562207467,
    paths=["version", "type", "license", "jatsxml"],
    transformation_ctx="DropFields_node",
)

# rename existing columns
ChangeSchema_node = ApplyMapping.apply(
    frame=DropFields_node,
    mappings=[
        ("doi", "string", "preprint_identifier", "string"),
        ("title", "string", "title", "string"),
        ("authors", "string", "authors", "string"),
        ("author_corresponding", "string", "author_corresponding", "string"),
        (
            "author_corresponding_institution",
            "string",
            "author_corresponding_institution",
            "string",
        ),
        ("date", "string", "latest_preprint_date", "string"),
        ("category", "string", "category", "string"),
        ("abstract", "string", "abstract", "string"),
        ("published", "string", "published_identifier", "string"),
        ("server", "string", "preprint_platform", "string"),
    ],
    transformation_ctx="ChangeSchema_node",
)


def get_word_count(doi: str, preprint_platform: str) -> [int, int]:
    regex_str = "CAR[\s-]T"
    res = requests.get(f"https://www.{preprint_platform}.org/content/{doi}.pdf")
    if res.status_code != 200:
        raise requests.exceptions.HTTPError(
            f"Request failed with status code {res.status_code} with doi {doi}"
        )
    else:
        doc = pymupdf.Document(stream=res.content)
        total_word_count = 0
        total_keyword_count = 0
        for page in doc:
            words = page.get_text("words")
            text = page.get_text("text")
            total_keyword_count += len(re.findall(regex_str, text))
            total_word_count += len(words)
        return [total_keyword_count, total_word_count]


get_word_count_udf = SqlFuncs.udf(get_word_count, ArrayType(IntegerType()))

# Get keyword count and total word count
df_with_word_count = (
    ChangeSchema_node.toDF()
    .select(
        "preprint_identifier",
        "preprint_platform",
        "title",
        "authors",
        "author_corresponding",
        "author_corresponding_institution",
        "latest_preprint_date",
        "published_identifier",
        "category",
        "abstract",
        *[
            get_word_count_udf("preprint_identifier", "preprint_platform")[i]
            for i in range(0, 2)
        ],
    )
    .toDF(
        "preprint_identifier",
        "preprint_platform",
        "title",
        "authors",
        "author_corresponding",
        "author_corresponding_institution",
        "latest_preprint_date",
        "published_identifier",
        "category",
        "abstract",
        "keyword_count",
        "total_word_count",
    )
)

# Add column with keyword
df_with_keyword = df_with_word_count.withColumn("keyword", SqlFuncs.lit("CAR[\s-]T"))

# Add date breakdown
df_with_dates = (
    df_with_keyword.withColumn(
        "preprint_day", date_format(substring("latest_preprint_date", 1, 10), "dd")
    )
    .withColumn(
        "preprint_month", date_format(substring("latest_preprint_date", 1, 10), "MM")
    )
    .withColumn(
        "preprint_short_month",
        date_format(substring("latest_preprint_date", 1, 10), "MMM"),
    )
    .withColumn(
        "preprint_year_quarter",
        date_format(substring("latest_preprint_date", 1, 10), "QQQ_yyyy"),
    )
    .withColumn(
        "preprint_year", date_format(substring("latest_preprint_date", 1, 10), "yyyy")
    )
    .withColumn(
        "preprint_day_of_week",
        date_format(substring("latest_preprint_date", 1, 10), "EEE"),
    )
)

# Dynamic Frame before partition
final_frame = DynamicFrame.fromDF(
    dataframe=df_with_dates, glue_ctx=glueContext, name="final_frame"
)

# Script generated for node Amazon S3
AmazonS3_node1731562842116 = glueContext.getSink(
    path="s3://resource-etl/cleaned/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["preprint_year", "preprint_month", "preprint_day"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1731562842116",
)
AmazonS3_node1731562842116.setCatalogInfo(
    catalogDatabase="cleanarxivdb", catalogTableName="cleanarxiv"
)
AmazonS3_node1731562842116.setFormat("glueparquet", compression="snappy")
AmazonS3_node1731562842116.writeFrame(final_frame)
job.commit()
