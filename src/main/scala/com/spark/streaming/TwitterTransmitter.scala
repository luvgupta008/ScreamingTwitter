package com.spark.streaming

import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}


/**
 *
 * Sends relevant key-value pairs from the Tweets and pass them to ElasticSearch
 *
 */
object TwitterTransmitter {
  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()

    val conf = ConfigFactory.parseFile(new File("config/TwitterScreamer.conf"))

    val credentials = conf.getConfig("oauthcredentials")
    val consumerKey = credentials.getString("consumerKey")
    val consumerSecret = credentials.getString("consumerSecret")
    val accessToken = credentials.getString("accessToken")
    val accessTokenSecret = credentials.getString("accessTokenSecret")
    val filters = if (args.length == 0) List() else args.toList

    println("Filters being used: " + filters.mkString(","))

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")


    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val tweetMap = stream.map(status => {

      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")


      val tweetMap =
        ("UserID" -> status.getUser.getId) ~
          ("UserDescription" -> status.getUser.getDescription) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount) ~
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~
          {
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
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText) ~
          ("TextLength" -> status.getText.length) ~
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


      def spamDetector(tweet: Map[String, Any]): Boolean = {
        {
          Days.daysBetween(new DateTime(formatter.parse(tweet.get("UserCreated").mkString).getTime),
            DateTime.now).getDays > 1
        } & {
          tweet.get("UserStatusCount").mkString.toInt > 50
        } & {
          tweet.get("UserFollowersRatio").mkString.toFloat > 0.01
        } & {
          tweet.get("UserDescription").mkString.length > 20
        } & {
          tweet.get("Text").mkString.split(" ").filter(_.startsWith("#")).length < 5
        } & {
          tweet.get("TextLength").mkString.toInt > 20
        } & {
          val filters = List("rt and follow","rt & follow","rt+follow","follow and rt","follow & rt","follow+rt")
          !filters.exists(tweet.get("Text").mkString.toLowerCase.contains)
        }
      }

      spamDetector(tweetMap.values) match {
        case true => tweetMap.values.+("spam" -> false)
        case _ => tweetMap.values.+("spam" -> true)
      }

    })

    tweetMap.print

//    tweetMap.map(s => List("Tweet Extracted")).print
 //   tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "spark/tweets", Map("es.mapping.timestamp" -> "StatusCreatedAt")) }

    ssc.start
    ssc.awaitTermination
  }
}