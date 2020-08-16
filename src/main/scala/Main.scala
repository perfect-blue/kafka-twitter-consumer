import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, length}
import Utillities._
import Schema._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object Main {

  val spark = SparkSession.builder()
    .appName("twitter-consumer")
    .master("local[2]")
    .getOrCreate()

  val bootstrapserver="slave1:9092,slave2:9092,slave3:9092"
  val topic="twitter-test"

  def readFromKafka(): Dataset[Row] ={
    val twitterDF: DataFrame= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapserver)
      .option("subscribe",topic)
      .load()


    setupLogging()
    twitterDF.printSchema()

    import spark.implicits._
    val tweetDF=twitterDF.select(from_json(expr("cast(value as string)"),payloadStruct) as ("tweet"))
    tweetDF.printSchema()
    val structuredTweetDF= tweetDF
        .select(
          $"tweet.payload.Id",
          $"tweet.payload.CreatedAt",
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
          $"tweet.payload.User.*",
          $"tweet.payload.Retweet",
          expr("tweet.payload.Lang as TweetLang")
        ).filter($"tweet.payload.Lang"==="in")
         .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL AND")


      structuredTweetDF
  }

  def writeToFile(structuredTweetDF: Dataset[Row])={
    structuredTweetDF
      .writeStream
      .format("csv")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(2.minutes)
      )
      .option("header",true)
      .option("truncate",false)
      .option("path","D:/dump/twitter/result")
      .option("checkpointLocation","D:/dump/twitter/result/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val tweet = readFromKafka()
    writeToFile(tweet)
  }
}
