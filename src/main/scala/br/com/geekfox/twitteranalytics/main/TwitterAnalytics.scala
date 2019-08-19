package br.com.geekfox.twitteranalytics.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.ml.PipelineModel

object TwitterAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TwitterAnalytics").master("local[*]").getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter")
      .option("failOnDataLoss", false)
      .load()

    val schema = StructType(
      Array(
        StructField("created_at", StringType, true),
        StructField("text", StringType, true),
        StructField("user", StructType(
          Array(
            StructField("screen_name", StringType, true),
            StructField("location", StringType, true)
          )
        ), true),
        StructField("entities", StructType(
          Array(
            StructField("hashtags", ArrayType(
              StructType(
                Array(
                  StructField("text", StringType, true)
                )
              )
            ), true)
          )
        ), true)
      )
    )

    //val string_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    /*val query = string_df.writeStream
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, version: Long): Boolean = {
          true
        }

        override def process(value: Row): Unit = {
          val string_value = value.getAs[String]("value")
          val json_parsed = JSON.parseFull(string_value).get.asInstanceOf[Map[String, Any]]
          println(json_parsed("text"))
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .start()*/
    /*schema.printTreeString()
    val json_df = df.select(from_json($"value".cast("string"), schema) as "value").select("value.entities.hashtags")
    val hashtags_df = json_df.select(explode(col("hashtags")).as("hashtag")).groupBy("hashtag").count().orderBy(desc("count"))
    hashtags_df.printSchema()
    val query = hashtags_df
        .selectExpr("hashtag.text AS key", "CAST(count AS STRING) AS value")
        .writeStream
        .format("kafka")
        //.format("console")
        .outputMode("complete")
        .option("topic", "hashtags")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()

    query.awaitTermination()
     */

    val model = PipelineModel.read.load("src/main/resources/model")

    val json_df = df.select(from_json($"value".cast("string"), schema) as "value").selectExpr("value.text AS tweet")

    val sentiment_df = model.transform(json_df)

    val query = sentiment_df
      .writeStream
      .format("console")
      .start()

    query.awaitTermination()



  }
}
