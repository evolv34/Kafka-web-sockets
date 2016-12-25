package com.websockets

import java.util.Properties

import scala.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.ValueMapper
import org.eclipse.jetty.websocket.api._
import com.google.gson.Gson
import java.util.UUID
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import com.websockets.utils.GenericAvroSerde
import com.websockets.utils.GenericAvroSerde

class KafkaConnector {
  
  def connect(metaData: MetaData): Connect = {
   	var connect: Connect = null;
    if(metaData.schemaUrl != null){
    	connect = new KafkaAvroConnect(metaData)
    }else {
      connect = new KafkaJsonConnect(metaData);
    }
    connect.connect();
    connect
  }
}