package com.websockets.producer

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaProducerWebsocket {

  implicit def mapToProperties(map: Map[String, String]): Properties = {
    var props = new Properties()
    for ((k, v) <- map) props.put(k, v)

    props
  }

  var propertiesMap: Map[String, String] =
    Map("bootstrap.servers" -> "172.17.0.1:9092",
      "acks" -> "all",
      "retries" -> "0",
      "batch.size" -> "16384",
      "linger.ms" -> "1",
      "buffer.memory" -> "33554432",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")

  var producer: Producer[String, String] = new KafkaProducer[String, String](propertiesMap)

  def send(message: String) = {
//    var jsonParser = Try(new JsonParser().parse(message.toString()))
    producer.send(new ProducerRecord[String, String]("test", message))
//    jsonParser match {
//      case Success(v) =>
//        
//        if (jsonParser.get.isJsonArray()) {
//          var iterator = jsonParser.get.getAsJsonArray.iterator()
//          while (iterator.hasNext()) {
//            var jsonElement = iterator.next().getAsJsonObject
//            producer.send(new ProducerRecord[String, String]("test-topic", jsonElement.toString()))
//          }
//        } else {
//          producer.send(new ProducerRecord[String, String]("test-topic", message))
//        }
//
//      case Failure(f) => producer.send(new ProducerRecord[String, String]("test", message))
//      
//    }

  }
}