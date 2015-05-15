package com.spark.streaming

import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}

/**
 *
 * Sends relevant key-value pairs from the Tweets and pass them to ElasticSearch
 * To run this project, you need to have elasticsearch installed and running
 * Need sbt, OAuth credentials in config file
 * From command line
 *
 * $ cd ScreamingTwitter
 * $ sbt run
 * or
 * $ sbt 'run filters'
 *
 */
object TwitterTransmitter {
  def main(args: Array[String]) {

    // Define Logging Level
    StreamingLogger.setStreamingLogLevels()

    // Read configurations from a config file
    val conf = ConfigFactory.parseFile(new File("config/TwitterScreamer.conf"))

    // Get the OAuth credentials from the configuration file
    val credentials = conf.getConfig("oauthcredentials")
    val consumerKey = credentials.getString("consumerKey")
    val consumerSecret = credentials.getString("consumerSecret")
    val accessToken = credentials.getString("accessToken")
    val accessTokenSecret = credentials.getString("accessTokenSecret")

    // Accept Twitter Stream filters from arguments passed while running the job
    val filters = if (args.length == 0) List() else args.toList

    // Print the filters being used
    println("Filters being used: " + filters.mkString(","))

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Initialize a SparkConf with all available cores
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
    // Automatically create index in Elasticsearch
    sparkConf.set("es.resource","sparksender/tweets")
    sparkConf.set("es.index.auto.create", "false")
    // Define the location of Elasticsearch cluster
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    // Create a StreamingContext with a batch interval of 10 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    // Create a DStream the gets streaming data from Twitter with the filters provided
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Process each tweet in a batch
    val tweetMap = stream.map(status => {

      // Defined a DateFormat to convert date Time provided by Twitter to a format understandable by Elasticsearch
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")

      // Creating a JSON using json4s.JSONDSL with fields from the tweet and calculated fields
      val tweetMap =
        ("UserID" -> status.getUser.getId) ~
          ("UserDescription" -> status.getUser.getDescription) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount) ~
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~
          // Ration is calculated as the number of followers divided by number of people followed
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~ {
          if (status.getGeoLocation != null)
            ("Geo_Latitude" -> status.getGeoLocation.getLatitude) ~ ("Geo_Longitude" -> status.getGeoLocation.getLongitude)
          else
            ("Geo_Latitude" -> "") ~ ("Geo_Longitude" -> "")
        } ~
          ("UserLang" -> status.getUser.getLang) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserVerification" -> status.getUser.isVerified) ~
          ("UserName" -> status.getUser.getName) ~
          ("UserStatusCount" -> status.getUser.getStatusesCount) ~
          // User Created DateTime is first converted to epoch miliseconds and then converted to the DateFormat defined above
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText) ~
          ("TextLength" -> status.getText.length) ~
          //Tokenized the tweet message and then filtered only words starting with #
          ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" ")) ~
          ("StatusCreatedAt" -> formatter.format(status.getCreatedAt.getTime)) ~ {
          if (status.getPlace != null) {
            if (status.getPlace.getName != null)
              ("PlaceName" -> status.getPlace.getName)
            else
              ("PlaceName" -> "")
          } ~ {
            if (status.getPlace.getCountry != null)
              ("PlaceCountry" -> status.getPlace.getCountry)
            else
              ("PlaceCountry" -> "")
          }
          else
            ("PlaceName" -> "") ~
              ("PlaceCountry" -> "")
        }

      // This function takes Map of tweet data and returns true if the message is not a spam
      def spamDetector(tweet: Map[String, Any]): Boolean = {
        {
          // Remove recently created users = Remove Twitter users who's profile was created less than a day ago
          Days.daysBetween(new DateTime(formatter.parse(tweet.get("UserCreated").mkString).getTime),
            DateTime.now).getDays > 1
        } & {
          // Users That Create Little Content =  Remove users who have only ever created less than 50 tweets
          tweet.get("UserStatusCount").mkString.toInt > 50
        } & {
          // Remove Users With Few Followers
          tweet.get("UserFollowersRatio").mkString.toFloat > 0.01
        } & {
          // Remove Users With Short Descriptions
          tweet.get("UserDescription").mkString.length > 20
        } & {
          // Remove messages with a Large Numbers Of HashTags
          tweet.get("Text").mkString.split(" ").filter(_.startsWith("#")).length < 5
        } & {
          // Remove Messages with Short Content Length
          tweet.get("TextLength").mkString.toInt > 20
        } & {
          // Remove Messages Requesting Retweets & Follows
          val filters = List("rt and follow", "rt & follow", "rt+follow", "follow and rt", "follow & rt", "follow+rt")
          !filters.exists(tweet.get("Text").mkString.toLowerCase.contains)
        }
      }
      // If the tweet passed through all the tests in SpamDetector Spam indicator = FALSE else TRUE
      spamDetector(tweetMap.values) match {
        case true => tweetMap.values.+("Spam" -> false)
        case _ => tweetMap.values.+("Spam" -> true)
      }

    })

    //tweetMap.print
    tweetMap.map(s => List("Tweet Extracted")).print

    // Each batch is saved to Elasticsearch with StatusCreatedAt as the default time dimension
    tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparksender/tweets", Map("es.mapping.timestamp" -> "StatusCreatedAt")) }

    ssc.start  // Start the computation
    ssc.awaitTermination  // Wait for the computation to terminate
  }
}
