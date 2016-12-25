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
import com.websockets.producer.KafkaProducerWebsocket
import com.websockets.producer.KafkaProducerWebsocket

@WebSocket
class KafkaProducerHandler {

  @OnWebSocketConnect
  def onConnect(subscriber: Session): Unit = {

  }

  @OnWebSocketMessage
  def onMessage(subscriber: Session, message: String): Unit = {
    var kafkaProducer: KafkaProducerWebsocket = new KafkaProducerWebsocket()
    
    kafkaProducer.send(message)
    
  }

  @OnWebSocketClose
  def onClose(subscriber: Session, statusCode: Int, reason: String): Unit = {
    removeSubscriber(subscriber)
  }

  def removeSubscriber(subscriber: Session): Unit = {
    subscriber.close()
  }

  def remove(subsriber: Session, list: Set[Session]): Set[Session] = list diff Set(subsriber)
}