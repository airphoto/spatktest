import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object StructureStreamingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.81.248.102:9092,10.29.254.50:9092,10.81.248.203:9092")
    .option("subscribe", "struct")
    .load()

    val words = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val wordCounts = words.flatMap(_._2.split(" ")).groupBy("value").agg(sum(length($"value")))
    val query = wordCounts.writeStream.outputMode("complete").option("checkpointLocation", "/test/output/checkpoint/structure").format("console").start()
//
//    query.awaitTermination()
    lines.show(false)
  }
}
