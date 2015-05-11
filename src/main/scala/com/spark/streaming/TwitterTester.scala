package com.spark.streaming

import java.io.File
import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
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
object TwitterTester {
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

      tweetRecord
    })

    ssc.start
    ssc.awaitTermination
  }
}