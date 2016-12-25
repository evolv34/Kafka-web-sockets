package com.websockets.api

import com.websockets.handler.KafkaHandler
import com.websockets.handler.KafkaProducerHandler

import spark.Spark.init
import spark.Spark.port
import spark.Spark.webSocket

object MainApp {

  def main(args: Array[String]): Unit = {
    		port(9093)
        webSocket("/kafka", classOf[KafkaHandler])
        webSocket("/produce", classOf[KafkaProducerHandler])
        init()
  }

}   