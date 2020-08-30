import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, length}
import Utillities._
import Schema._
import com.sun.corba.se.internal.CosNaming.BootstrapServer
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.TimestampType

import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {
    val spark=initializeSession

    //Input
    val topic = args(0)
    val bootstrapserver=args(1)
    val duration=args(2)
    val format=args(3)
    val path=args(4)

    val trigger=duration.toInt.seconds
    val tweet=readFromKafka(spark,topic,bootstrapserver)
    val file =writeToFile(tweet, format,
      path+"/result/"+format,
      path+"/checkpoint",
      trigger)

    val console=writeToConsole(tweet,trigger)

    file.awaitTermination()
    console.awaitTermination()
  }


  def initializeSession:SparkSession={
    val spark = SparkSession.builder()
      .appName("twitter-consumer")
      .getOrCreate()

    spark
  }

  /**
   * read twitter data stream from kafka
   * @return
   */
  def readFromKafka(spark:SparkSession,topic:String,bootstrapServer: String): Dataset[Row] ={
    /**
     * Read Stream
     */
    val twitterDF: DataFrame= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapServer)
      .option("subscribe",topic)
      .option("startingoffsets","latest")
      .load()

    //clear logs
    setupLogging()

    //register UDF
    val getYear=spark.udf.register("getYear",year)
    val getMonth=spark.udf.register("getYear",month)
    val getDay=spark.udf.register("getYear",day)

    //transform stream
    import spark.implicits._
    val tweetDF=twitterDF.select(col("offset"),from_json(expr("cast(value as string)"),payloadStruct) as ("tweet"))
    val structuredTweetDF= tweetDF
        .select(
          $"offset",
          $"tweet.payload.Id",
          to_timestamp(from_unixtime(col("tweet.payload.CreatedAt").divide(1000))) as("CreatedAt"),
          $"tweet.payload.Text",
          $"tweet.payload.Source",
          $"tweet.payload.InReplyToStatusId",
          $"tweet.payload.InReplyToUserId",
          $"tweet.payload.InReplyToScreenName",
          $"tweet.payload.GeoLocation.Latitude",
          $"tweet.payload.GeoLocation.Longitude",
          $"tweet.payload.Place.Name",
          $"tweet.payload.Place.StreetAddress",
          $"tweet.payload.Place.CountryCode",
          $"tweet.payload.Place.Id",
          $"tweet.payload.Place.Country",
          $"tweet.payload.Place.PlaceType",
          $"tweet.payload.Place.URL",
          $"tweet.payload.Place.FullName",
          $"tweet.payload.Favorited",
          $"tweet.payload.Retweeted",
          $"tweet.payload.FavoriteCount",
          $"tweet.payload.User.id".as("UserID"),
          $"tweet.payload.User.Name".as("UserName"),
          $"tweet.payload.User.ScreenName".as("ScreenName"),
          $"tweet.payload.User.Location".as("Location"),
          $"tweet.payload.User.Name".as("Description"),
          $"tweet.payload.User.Name".as("ContributorsEnabled"),
          $"tweet.payload.User.OriginalProfileImageURL".as("ProfileImageURL"),
          $"tweet.payload.User.URL".as("URL"),
          $"tweet.payload.User.Protected".as("Protected"),
          $"tweet.payload.User.FollowersCount",
          $"tweet.payload.User.FriendsCount",
          to_timestamp(from_unixtime($"tweet.payload.User.CreatedAt".cast("long").divide(1000))) as("AccountCreatedAt"),
          $"tweet.payload.User.FavoriteCount",
          $"tweet.payload.User.UtcOffset",
          $"tweet.payload.User.TimeZone",
          $"tweet.payload.User.StatusesCount",
          $"tweet.payload.User.GeoEnabled",
          $"tweet.payload.User.Verified",
          $"tweet.payload.User.Lang".as("UserLang"),
          $"tweet.payload.Retweet",
          expr("tweet.payload.Lang"),
          expr("cast(tweet.payload.UserMentionEntities.Name as String)") as("Metions"),
          expr("cast(tweet.payload.HashtagEntities.Text as String)") as ("Hashtags"),
          $"tweet.payload.RetweetCount",
          $"tweet.payload.RetweetedByMe",
          $"tweet.payload.PossiblySensitive",
          expr("cast(tweet.payload.withheldInCountries as String)") as ("witheldIn")
        ).filter($"tweet.payload.Lang"==="in")
         .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL AND")

      structuredTweetDF
        .withColumn("Year",getYear(col("CreatedAt")))
        .withColumn("Month",getMonth(col("CreatedAt")))
        .withColumn("Day",getDay(col("CreatedAt")))

  }

  /**
   * Write Twitter to HDFS
   * @param structuredTweetDF, Dataset that want to be written
   * @param format, what is the format "CSV" or "Parquet"
   * @param path, to which directory you want to write your data
   * @param checkpoint, metadata location of structured streaming
   * @param trigger, how long you should wait before the data is written
   * @return StreamingQuery
   */
  def writeToFile(structuredTweetDF: Dataset[Row],format :String, path:String, checkpoint:String,trigger:FiniteDuration): StreamingQuery={
    val result=structuredTweetDF
      .writeStream
      .format(format)
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(trigger)
      )
      .partitionBy("Year","Month","Day")
      .option("header",true)
      .option("truncate",false)
      .option("path",path)
      .option("checkpointLocation",checkpoint)
      .start()

    result
  }

  /**
   * Write Twitter data to console
   * @param structuredTweetDF
   * @param trigger
   * @return
   */
  def writeToConsole(structuredTweetDF: Dataset[Row],trigger: FiniteDuration): StreamingQuery={
    val result= structuredTweetDF
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(trigger)
      )
      .option("truncate",false)
      .start()

    result
  }

  /**
   * User-defined function
   */
  def year:String=>String=(column:String)=>{
    val dateTime=column.split(" ")
    val dates=dateTime(0).split("-")
    dates(0)
  }

  def month:String=>String=(column:String)=>{
    val dateTime=column.split(" ")
    val dates=dateTime(0).split("-")
    dates(1)
  }

  def day:String=>String=(column:String)=>{
    val dateTime=column.split(" ")
    val dates=dateTime(0).split("-")
    dates(2)
  }

}
