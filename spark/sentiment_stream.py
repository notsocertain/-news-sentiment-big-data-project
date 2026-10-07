from pyspark.sql import SparkSession
from pymongo import MongoClient

from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

from sentiment_models import score_sentiment_batch
from sentiment_pipeline import (
    _ANALYSIS_SCHEMA,
    deduplicate_article_records,
    score_article_records,
)

# Create Spark session
spark = SparkSession.builder \
    .appName("RealTimeNewsSentiment") \
    .getOrCreate()
    # .config("spark.mongodb.output.uri", "mongodb://mongo:27017/newsdb.sentiments") \
    # .master("local[*]") \

schema = StructType([
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("link", StringType()),
    StructField("source", StringType()),
    StructField("region", StringType()),
    StructField("published_at", StringType()),
    StructField("ingested_at", StringType())
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "news_topic") \
    .option("maxOffsetsPerTrigger", 5) \
    .option("startingOffsets", "earliest") \
    .load()
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("published_at", to_timestamp(col("published_at"))) \
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))

mongo_client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=30000)
sentiment_collection = mongo_client["newsdb"]["sentiments"]
_title_index_ready = False


def process_microbatch(batch_df, _batch_id):
    global _title_index_ready
    articles = [row.asDict(recursive=True) for row in batch_df.coalesce(1).collect()]
    if not articles:
        return

    if not _title_index_ready:
        sentiment_collection.create_index("title")
        _title_index_ready = True

    titles = list(dict.fromkeys(article.get("title") for article in articles))
    existing_titles = {
        stored.get("title")
        for stored in sentiment_collection.find(
            {"title": {"$in": titles}},
            {"_id": 0, "title": 1},
        )
    }
    new_articles = deduplicate_article_records(articles, existing_titles)
    if not new_articles:
        return

    scored_articles = score_article_records(new_articles, score_sentiment_batch)
    output_schema = StructType(batch_df.schema.fields + _ANALYSIS_SCHEMA.fields)
    output_df = batch_df.sparkSession.createDataFrame(scored_articles, output_schema)
    output_df.coalesce(1).write.format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.connection.uri", "mongodb://mongo:27017/newsdb.sentiments") \
        .option("spark.mongodb.output.uri", "mongodb://mongo:27017/newsdb.sentiments") \
        .option("database", "newsdb") \
        .option("collection", "sentiments") \
        .save()


query = json_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_microbatch) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()
query.awaitTermination()
