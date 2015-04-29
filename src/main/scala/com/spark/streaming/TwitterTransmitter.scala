package com.spark.streaming

import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._


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
    val filters = List()

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

      def getValStr(x: Any): String = {
        if (x != null && !x.toString.isEmpty) x.toString + "|" else "|"
      }


      val tweetRecord =
        getValStr(status.getUser.getId) +
          getValStr(status.getUser.getScreenName) +
          getValStr(status.getUser.getFriendsCount) +
          getValStr(status.getUser.getFavouritesCount) +
          getValStr(status.getUser.getFollowersCount) +
          getValStr(status.getUser.getLang) +
          getValStr(status.getUser.getLocation) +
          getValStr(status.getUser.getName) +
          getValStr(status.getId) +
          getValStr(status.getCreatedAt) +
          getValStr(status.getGeoLocation) +
          getValStr(status.getPlace) +
          getValStr(status.getText) +
          getValStr(status.getInReplyToUserId) +
          getValStr(status.getPlace) +
          getValStr(status.getRetweetCount) +
          getValStr(status.getRetweetedStatus) +
          getValStr(status.getSource) +
          getValStr(status.getInReplyToScreenName) +
          getValStr(status.getText)

      val tweetMap =
        ("UserID" -> status.getUser.getId) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount)
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~ {
          if (status.getGeoLocation != null)
            ("Geo_Latitude" -> status.getGeoLocation.getLatitude) ~ ("Geo_Longitude" -> status.getGeoLocation.getLongitude)
          else
            ("Geo_Latitude" -> "") ~ ("Geo_Longitude" -> "")
        } ~
          ("UserLang" -> status.getUser.getLang) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserName" -> status.getUser.getName) ~
          ("Text" -> status.getText) ~
          ("CreatedAt" -> status.getCreatedAt.toString)


      tweetMap.values
    })

    tweetMap.map(status => List()).print
    tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "spark/docs", Map("es.mapping.timestamp","CreatedAt")) }

    ssc.start
    ssc.awaitTermination
  }
}