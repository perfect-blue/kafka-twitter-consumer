import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, length}
import Utillities._

object Main {

  val spark = SparkSession.builder()
    .appName("twitter-consumer")
    .master("local[2]")
    .getOrCreate()

  val bootstrapserver="slave1:9092,slave2:9092,slave3:9092"
  val topic="twitter-test"

  def readFromKafka() ={
    val twitterDF: DataFrame= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapserver)
      .option("subscribe",topic)
      .load()

    setupLogging()

    twitterDF
      .select(col("topic"),expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate",false)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
  }
}
