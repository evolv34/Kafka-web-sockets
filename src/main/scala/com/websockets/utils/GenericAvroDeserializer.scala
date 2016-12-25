package com.websockets.utils

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer



class GenericAvroDeserializer extends Deserializer[GenericRecord]{
    	
     var inner: KafkaAvroDeserializer = null;
     var schemaRegistryUrl:String = null;
     
     def this(schemaRegistryUrl: String){
       this();
       inner = new KafkaAvroDeserializer();
       val config: Map[String,String]  = new HashMap[String, String]();
       this.schemaRegistryUrl = schemaRegistryUrl;
         config.put("schema.registry.url", schemaRegistryUrl);
         inner.configure(config, false);
     }
     
     def this(client : SchemaRegistryClient) {
       this();
    	 inner = new KafkaAvroDeserializer(client);
     }

    def this (client: SchemaRegistryClient , props: Map[String, _] ) {
        this();
        inner = new KafkaAvroDeserializer(client, props);
    }

    @Override
    def configure(configs: Map[String, _], isKey: Boolean) {
        inner.configure(configs, isKey);
    }

    @Override
    def deserialize(s: String, bytes: Array[Byte]): GenericRecord = {
      inner.deserialize(s, bytes).asInstanceOf[GenericRecord]
      
    }
    

    @Override
    def close() {
        inner.close();
    }
}