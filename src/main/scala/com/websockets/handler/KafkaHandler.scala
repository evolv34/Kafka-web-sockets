package com.websockets.handler

import scala.collection.mutable.HashMap

import org.eclipse.jetty.websocket.api.Session
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage
import org.eclipse.jetty.websocket.api.annotations.WebSocket

import com.google.gson.Gson

import com.websockets.KafkaConnector
import com.websockets.MetaData
import com.websockets.params


@WebSocket
class KafkaHandler {

  @OnWebSocketConnect
  def onConnect(subscriber: Session): Unit = {
  }

  @OnWebSocketMessage
  def onMessage(subscriber: Session, message: String): Unit = {
    println("Message " + message)

    var metaData: MetaData = new Gson().fromJson(message, classOf[MetaData])

    var messageType = metaData.messageType;
    var consumerGroup: String = metaData.consumerGroup;
    var topic: String = metaData.topic;

    if (params.subscriberMetaData.contains(subscriber)){
      removeSubscriber(subscriber);
    }
    if (!params.subscriber.contains(consumerGroup)) {
      println("Consumer group doesnot exists")
      params.subscriber.put(consumerGroup, new HashMap[String, Set[Session]]())
      params.connectGroupMap.put(consumerGroup, new HashMap[String, KafkaConnector]())
    }
    if (!params.subscriber.get(consumerGroup).get.contains(topic)) {
      params.subscriber.get(consumerGroup)
        .get
        .put(topic, Set[Session]())

        var kafkaConnect: KafkaConnector = new KafkaConnector();
        kafkaConnect.connect(metaData);        
        
      params.connectGroupMap.get(consumerGroup).get.put(topic, kafkaConnect)

    } else {
      params.connectGroupMap.get(consumerGroup).get.get(topic).get.connect(metaData);
    }
    
    params.subscriber.get(consumerGroup)
      .get
      .put(topic, params.subscriber.get(consumerGroup)
        .get(topic) + subscriber)

    println("Number of Subscribers " + params.subscriber.get(consumerGroup)
      .get
      .get(topic).get.size)

    params.subscriberMetaData.put(subscriber, String.format("%s-%s", topic,consumerGroup))
  }

  @OnWebSocketClose
  def onClose(subscriber: Session, statusCode: Int, reason: String): Unit = {
    removeSubscriber(subscriber)
  }
  
  def removeSubscriber(subscriber: Session): Unit = {
    var topicConsumerGroup: String = params.subscriberMetaData.get(subscriber).get
    var topic = topicConsumerGroup.split("-")(0)
    var consumerGroup = topicConsumerGroup.split("-")(1)
    
    val updatedSubscriberList: Set[Session] = remove(subscriber, params.subscriber.get(consumerGroup)
                                                                                  .get
                                                                                  .get(topic)
                                                                                  .get)
    params.subscriber.get(consumerGroup).get.put(topic, updatedSubscriberList)
  }


  def remove(subsriber: Session, list: Set[Session]): Set[Session] = list diff Set(subsriber)
}