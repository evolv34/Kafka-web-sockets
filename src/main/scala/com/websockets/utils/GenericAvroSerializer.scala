package com.websockets.utils

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

class GenericAvroSerializer extends Serializer[GenericRecord] {
  var inner: KafkaAvroSerializer = null;
  
	/**
	 * Constructor used by Kafka Streams.
	 */
	def this(schemaRegistryUrl: String) {
	  this();
		inner = new KafkaAvroSerializer();
		val config: Map[String, String] = new HashMap[String, String]();
		config.put("schema.registry.url", schemaRegistryUrl);
		inner.configure(config, false);
	}

	def this(client: SchemaRegistryClient) {
	  this();
		inner = new KafkaAvroSerializer(client);
	}

	@Override
	def configure(configs: Map[String, _] , isKey: Boolean): Unit = inner.configure(configs, isKey);
	

	@Override
	def serialize(topic: String , record: GenericRecord): Array[Byte] = inner.serialize(topic, record);

	@Override
	def close() = inner.close();
}