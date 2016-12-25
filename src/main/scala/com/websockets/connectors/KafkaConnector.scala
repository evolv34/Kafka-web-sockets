package com.websockets.connectors

import com.websockets.MetaData

class KafkaConnector {

  def connect(metaData: MetaData): Connect = {
    var connect: Connect = null;
    if (metaData.schemaUrl != null) {
      connect = new KafkaAvroConnect(metaData)
    } else {
      connect = new KafkaJsonConnect(metaData);
    }
    connect.connect();
    connect
  }
}