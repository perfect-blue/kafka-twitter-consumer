import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Utillities._

object Main {

  val spark=SparkSession.builder()
    .appName("Twitter-Consumer")
    .getOrCreate()

  def readFromKafka(bootstrapservers:String, topic:String)={
    val kafkaDF:DataFrame= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapservers)
      .option("subscribe",topic)
      .load()

    setupLogging()

    kafkaDF
      .select(col("topic"),expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
  def main(args: Array[String]): Unit = {
    val bootstrapserver=args(0)
    val topic=args(1)

    readFromKafka(bootstrapserver,topic)
  }
}
