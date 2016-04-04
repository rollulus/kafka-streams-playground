package com.github.rollulus.myprocessor

import java.text.SimpleDateFormat

import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer, LongSerializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{KStreamBuilder}
import KeyValueImplicits._
import Properties._

object TweetsPerMinuteEventTime {
  lazy val SOURCE_TOPIC_CONFIG = "source.topic"
  lazy val SINK_TOPIC_CONFIG = "sink.topic"

  def propertiesFromFiles(files: Array[String]) = files.map(Properties.fromFile).foldLeft(new java.util.Properties)(Properties.union)

  def main(args: Array[String]): Unit = {
    // configure
    require(args.length > 0, "at least one .properties file should be given as program argument")
    val builder = new KStreamBuilder
    val cfg = propertiesFromFiles(args).union(fixedProperties())
    val sourceTopic = cfg.getProperty(SOURCE_TOPIC_CONFIG)
    val sinkTopic = cfg.getProperty(SINK_TOPIC_CONFIG)

    // source
    val tweets = builder.stream[String, TwitterStatus](sourceTopic)

    // transformation
    // per/minute is at event time, updated every minute with non-overlapping windows
    val tweetcount = tweets.map[String, TwitterStatus]((k, v) => {
      val dt = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(v.getCreatedAt);
      (new SimpleDateFormat("EEE MMM dd HH:mm:00 Z yyyy").format(dt), v)
    }).countByKey(new StringSerializer, new LongSerializer, new StringDeserializer, new LongDeserializer, "Count")

    // sink
    tweetcount.to(sinkTopic, new StringSerializer, new LongSerializer)

    // run
    new KafkaStreams(builder, cfg).start()
  }
}
