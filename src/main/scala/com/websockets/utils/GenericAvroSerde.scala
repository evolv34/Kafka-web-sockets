package com.websockets.utils;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

class GenericAvroSerde extends Serde[GenericRecord] {
  
  var inner: Serde[GenericRecord] = null;

  def this(schemaRegistryUrl:String) {
    this();
    inner = Serdes.serdeFrom(new GenericAvroSerializer(schemaRegistryUrl), new GenericAvroDeserializer(schemaRegistryUrl));
  }

  def this(client:SchemaRegistryClient, props: Map[String, _]) {
    this();
    inner = Serdes.serdeFrom(new GenericAvroSerializer(client), new GenericAvroDeserializer(client, props));
  }

  def this(client: SchemaRegistryClient) {
	  this(client, Collections.emptyMap());
  }
  
  @Override
  def serializer(): Serializer[GenericRecord] = inner.serializer();
  
  @Override
  def deserializer(): Deserializer[GenericRecord] = inner.deserializer();
  
  @Override
  def configure(configs: Map[String, _], isKey: Boolean): Unit = {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  def close(): Unit = {
    inner.serializer().close();
    inner.deserializer().close();
  }

}