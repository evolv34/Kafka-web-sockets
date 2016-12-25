package com.websockets.handler

import org.eclipse.jetty.websocket.api.Session
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage
import org.eclipse.jetty.websocket.api.annotations.WebSocket

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.websockets.MetaData
import com.websockets.Params
import com.websockets.connectors.KafkaConnector
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable.{ Map }

@WebSocket
class KafkaHandler {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  @OnWebSocketConnect
  def onConnect(subscriber: Session): Unit = {
  }

  @OnWebSocketMessage
  def onMessage(subscriber: Session, message: String): Unit = {

    println("Message " + message)
    var metaData: MetaData = null
    try {
      metaData = mapper.readValue(message, classOf[MetaData])

      var messageType = metaData.messageType
      var consumerGroup: String = metaData.consumerGroup
      var topic: String = metaData.topic

      if (!Params.subscriber.contains(consumerGroup)) {
        Params.subscriber += (consumerGroup -> Map[String, Set[Session]]())
        Params.connectGroupMap += (consumerGroup -> Map[String, KafkaConnector]())
      }
      if (!Params.subscriber(consumerGroup).contains(topic)) {
        println("topic doesnot exists")
        Params.subscriber(consumerGroup) += (topic -> Set[Session]())

        var kafkaConnect = new KafkaConnector()
        kafkaConnect.connect(metaData);

        Params.connectGroupMap(consumerGroup) += (topic -> kafkaConnect)

      } else {
        Params.connectGroupMap(consumerGroup)(topic).connect(metaData)
      }

      Params.subscriber(consumerGroup) += (topic -> (Params.subscriber(consumerGroup)(topic) + subscriber))
      Params.subscriberMetaData += (subscriber -> s"$topic-$consumerGroup")

      println(s"Number of Subscribers ${Params.subscriber(consumerGroup)(topic).size}")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  @OnWebSocketClose
  def onClose(subscriber: Session, statusCode: Int, reason: String): Unit = {
    removeSubscriber(subscriber)
  }

  def removeSubscriber(subscriber: Session): Unit = {
    def remove(subsriber: Session, list: Set[Session]): Set[Session] = list diff Set(subsriber)

    val topicConsumerGroup = Params.subscriberMetaData(subscriber)
    val topic = topicConsumerGroup.split("-")(0)
    val consumerGroup = topicConsumerGroup.split("-")(1)

    val updatedSubscriberList: Set[Session] = remove(subscriber, Params.subscriber(consumerGroup)(topic))
    Params.subscriber(consumerGroup) += (topic -> updatedSubscriberList)
  }

}