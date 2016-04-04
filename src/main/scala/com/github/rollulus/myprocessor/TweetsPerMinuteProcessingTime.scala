package com.github.rollulus.myprocessor

import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer, LongSerializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.internals.WindowedSerializer
import org.apache.kafka.streams.kstream.{HoppingWindows, KStreamBuilder}
import KeyValueImplicits._
import Properties._

object TweetsPerMinuteProcessingTime {
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
    // per/minute is at processing time, update every second with overlapping windows
    val tweetcount = tweets.map[String, TwitterStatus]((k, v) => {
            ("somekey", v)
          }).countByKey(HoppingWindows.of("x").`with`(60*1000).every(1*1000),new StringSerializer, new LongSerializer, new StringDeserializer, new LongDeserializer)

    // sink
    tweetcount.to(sinkTopic, new WindowedSerializer[String](new StringSerializer), new LongSerializer)

    // run
    new KafkaStreams(builder, cfg).start()
  }
}
