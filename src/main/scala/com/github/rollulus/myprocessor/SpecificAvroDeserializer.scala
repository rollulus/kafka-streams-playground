package com.github.rollulus.myprocessor

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.Deserializer

class SpecificAvroDeserializer[T <: org.apache.avro.specific.SpecificRecord] extends Deserializer[T] with Configurable {
  private[myprocessor] var inner: KafkaAvroDeserializer = null

  def this(client: SchemaRegistryClient) {
    this()
    inner = new KafkaAvroDeserializer(client)
  }

  def this(client: SchemaRegistryClient, props: java.util.Map[String, _]) {
    this()
    inner = new KafkaAvroDeserializer(client, props)
  }

  def configure(configs: java.util.Map[String, _], isKey: Boolean) {
    inner = new KafkaAvroDeserializer
    inner.configure(configs, isKey)
  }

  def configure(configs: java.util.Map[String, _]) {
    inner = new KafkaAvroDeserializer
    inner.configure(configs, false)
  }

  @SuppressWarnings(Array("unchecked")) def deserialize(s: String, bytes: Array[Byte]): T = {
    return inner.deserialize(s, bytes).asInstanceOf[T]
  }

  def close {
    inner.close
  }
}