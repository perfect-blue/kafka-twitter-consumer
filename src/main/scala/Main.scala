import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, length}
import Utillities._
import Schema._
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.TimestampType

import scala.concurrent.duration._

object Main {

  val trigger=5.minutes
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
//    twitterDF.printSchema()

    import spark.implicits._
    val tweetDF=twitterDF.select(col("offset"),from_json(expr("cast(value as string)"),payloadStruct) as ("tweet"))
    tweetDF.printSchema()
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
          expr("tweet.payload.Lang as TweetLang"),
          expr("cast(tweet.payload.UserMentionEntities.Name as String)") as("Metions"),
          expr("cast(tweet.payload.HashtagEntities.Text as String)") as ("Hashtags"),
          $"tweet.payload.RetweetCount",
          $"tweet.payload.RetweetedByMe",
          $"tweet.payload.PossiblySensitive",
          expr("cast(tweet.payload.withheldInCountries as String)") as ("witheldIn")

        ).filter($"tweet.payload.Lang"==="in")
         .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL AND")

      structuredTweetDF.printSchema()
      structuredTweetDF
  }

  def writeToCSV(structuredTweetDF: Dataset[Row])={
    structuredTweetDF
      .writeStream
      .format("csv")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(trigger)
      )
      .partitionBy("CreatedAt")
      .option("header",true)
      .option("truncate",false)
      .option("path","D:/dump/twitter/result")
      .option("checkpointLocation","D:/dump/twitter/result/checkpoint")
      .start()
      .awaitTermination()
  }

  def writeToConsole(structuredTweetDF: Dataset[Row])={
    structuredTweetDF
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(trigger)
      )
      .option("truncate",false)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val tweet = readFromKafka()
//    writeToCSV(tweet)
    writeToConsole(tweet)
  }
}
