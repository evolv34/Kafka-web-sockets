package com.websockets

import scala.collection.mutable.{ Map, HashMap }
import org.eclipse.jetty.websocket.api._;
import com.websockets.connectors.KafkaConnector

object Params {
  val subscriber: Map[String, Map[String, Set[Session]]] = Map[String, Map[String, Set[Session]]]()
  val connectGroupMap: Map[String, Map[String, KafkaConnector]] = Map[String, Map[String, KafkaConnector]]()

  val subscriberMetaData: Map[Session, String] = Map[Session, String]()
}