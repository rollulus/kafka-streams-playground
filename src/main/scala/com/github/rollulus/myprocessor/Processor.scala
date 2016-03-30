package com.github.rollulus.myprocessor

import java.util.Properties

import io.confluent.kafka.serializers.{KafkaAvroDeserializerConfig, AbstractKafkaAvroSerDeConfig}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams._

/*class GenericAvroDeserializerX(client: SchemaRegistryClient, props: util.Map[String,_]) extends Deserializer[GenericRecord] {
  var inner : KafkaAvroDeserializer = new KafkaAvroDeserializer() //new KafkaAvroDeserializer(client, props)

  def this() = this(null, null)

  override def configure(map: util.Map[String, _], b: Boolean): Unit = inner.configure(map, b)
  override def close(): Unit = inner.close()
  override def deserialize(s: String, bytes: Array[Byte]): GenericRecord = {
    println(javax.xml.bind.DatatypeConverter.printHexBinary(bytes))
    inner.deserialize(s, bytes).asInstanceOf[GenericRecord]
  }
}*/

object Processor {
  def main(args: Array[String]): Unit = {

    val builder: KStreamBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.JOB_ID_CONFIG, "my-processor")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
      settings.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      settings.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      settings.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      settings.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[SpecificAvroDeserializer[TwitterStatus]])
      settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      settings.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
      settings
    }

    val lines = builder.stream[String, TwitterStatus]("tweets")

    val d = lines.mapValues[String](l=>{
      l.getText
    })
    d.to("tweets-text-only")

    new KafkaStreams(builder, streamingConfig).start()
  }
}