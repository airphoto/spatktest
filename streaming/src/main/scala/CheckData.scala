import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQueryListener}

object CheckData {
  def main(args: Array[String]): Unit = {
    val Array(servers,redisHost,port,password,db) = args
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", "original_server_logs,original_cp_logs,user-asset-change-v1")
      .load()

    val castData = lines
      .selectExpr("topic","CAST(key AS STRING)","CAST(value AS STRING)")
      .as[(String,String,String)]

    val serverSelectData = castData.filter($"topic"==="original_server_logs" || $"topic"==="original_cp_logs").select(
        regexp_extract($"value",s"""\\"type\\"\\s*:\\s*\\"(\\w+)\\"""",1).as("type"),
        regexp_extract($"value",s"""\\"time\\"\\s*:\\s*\\"(\\d{13})\\"""",1).as("time")).select(
        from_unixtime(substring($"time",0,10),"yyyy-MM-dd").as("target_day"),
        $"type")

    val serverCountData = serverSelectData.groupBy($"type",$"target_day").count()

    val assetSelectData = castData.
      filter($"topic"==="user-asset-change-v1").select(
        substring(regexp_extract($"value",s"""\\"createTime\\"\\s*:\\s*\\"(.+?)\\"""",1),0,10).cast("date").as("target_day"),
        when($"value".isNotNull,"asset").as("type"))
      .select($"target_day",$"type")

    val assetCountData = assetSelectData
      .groupBy($"type",$"target_day")
      .count()

    val writer = RedisWriter(redisHost,port.toInt,password,db.toInt)

    val assetQuery = assetCountData.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "/test/output/checkpoint/asset_count")
      .foreach(writer)
      .trigger(ProcessingTime("10 seconds"))
      .queryName("asset_query")
      .start()

    val serverQuery = serverCountData.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "/test/output/checkpoint/server_count")
      .foreach(writer)
      .trigger(ProcessingTime("10 seconds"))
      .queryName("server_query")
      .start()

    spark.streams.awaitAnyTermination()

  }
}
