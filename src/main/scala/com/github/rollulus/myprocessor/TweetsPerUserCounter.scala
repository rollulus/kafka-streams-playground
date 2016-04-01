package com.github.rollulus.myprocessor

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStreamBuilder}
import org.apache.kafka.streams._
import KeyValueImplicits._
import Properties._

object TweetsPerUserCounter {
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
    val tweetcount = tweets.map[String, TwitterStatus]((k, v) => {
      (v.getUser().getScreenName, v)
    }).countByKey(new StringSerializer, new LongSerializer, new StringDeserializer, new LongDeserializer, "Count")

    // sink
    tweetcount.to(sinkTopic, new StringSerializer, new LongSerializer)

    // run
    new KafkaStreams(builder, cfg).start()
  }
}

object fixedProperties {
  def apply() = Properties.create(Map(
    StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[SpecificAvroDeserializer[TwitterStatus]],
    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> "true"))
}

object KeyValueImplicits {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}
