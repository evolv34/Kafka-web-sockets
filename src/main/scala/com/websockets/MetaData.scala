package com.websockets

case class MetaData(
  fromBeginning: Boolean,
  messageType: String,
  consumerGroup: String,
  topic: String,
  kafkaUrl: String,
  zookeeperUrl: String,
  schemaUrl: String)