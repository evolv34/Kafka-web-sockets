package com.websockets

import scala.collection.mutable.{Map,HashMap}
import org.eclipse.jetty.websocket.api._;

object params {
  val subscriber:Map[String,Map[String, Set[Session]]] = new HashMap[String,Map[String,Set[Session]]]()
  val connectGroupMap:Map[String,Map[String, KafkaConnector]] = new HashMap[String,Map[String,KafkaConnector]]()
  
  val subscriberMetaData:Map[Session, String] = new HashMap[Session, String]();
}