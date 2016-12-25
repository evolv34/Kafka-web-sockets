package com.websockets

import java.util.Properties
import java.util.UUID

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.ForeachAction
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import com.websockets.utils.GenericAvroSerde

class KafkaAvroConnect(metaData:MetaData) extends Connect{
    val metaDataLocal = metaData;
    
    var streamConfig: StreamsConfig = new StreamsConfig(getProperties(metaDataLocal.kafkaUrl,
                                                                  metaDataLocal.zookeeperUrl, 
                                                                  metaDataLocal.consumerGroup,
                                                                  metaDataLocal.fromBeginning,
                                                                  metaDataLocal.schemaUrl))
    
    var kafkaStreams: KafkaStreams = null;
  
   def onMessageReceivedFromTopic(message: Object, metaData: MetaData): Unit = {
    params.subscriber.get(metaData.consumerGroup)
                      .get(metaData.topic)
                      .par
                      .foreach { subscriber => subscriber.getRemote.sendString(message.toString()) }
  }
   
   def connect(){
    
     var kStreamBuilder: KStreamBuilder = new KStreamBuilder()
     
     var kstream: KStream[String, GenericRecord] = kStreamBuilder.stream(Serdes.String(), new GenericAvroSerde(metaDataLocal.schemaUrl) ,metaDataLocal.topic)
      kstream.foreach(new ForeachAction[String, GenericRecord]() {

  			@Override
  			def apply(key: String, value: GenericRecord):Unit = {
  				 onMessageReceivedFromTopic(value, metaDataLocal)
  			}
      });
          
  		
  	  kafkaStreams = new KafkaStreams(kStreamBuilder, streamConfig)
      kafkaStreams.start()
      
   }
   
   def close():Unit = {
     kafkaStreams.close();
   }
   
   def getProperties(kafkaUrl: String, zookeeper: String, consumerGroup: String, fromBeginning: Boolean, schemaUrl: String): Properties = {
		var props:Properties = new Properties();
		var processorJob: String = UUID.randomUUID().toString()
		
		props.put(StreamsConfig.CLIENT_ID_CONFIG, processorJob);
		props.put("group.id", consumerGroup);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, processorJob);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
//		props.put("num.stream.threads", "3")
//		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde]);
		
		if (fromBeginning){
		  props.put("auto.offset.reset", "earliest");
		}else {
		  props.put("auto.offset.reset", "latest");
		}

		return props;
	}
}