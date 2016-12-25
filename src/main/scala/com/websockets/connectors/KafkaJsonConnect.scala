package com.websockets.connectors

import java.util.Properties
import java.util.UUID

import scala.util.Random

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.ValueMapper

import com.websockets.{ MetaData, Params }

class KafkaJsonConnect(metaData: MetaData) extends Connect {

  val metaDataLocal: MetaData = metaData;
  val consumerPicker: Random = new Random;

  var streamConfig: StreamsConfig =
    new StreamsConfig(getProperties(
      metaDataLocal.kafkaUrl,
      metaDataLocal.zookeeperUrl,
      metaDataLocal.consumerGroup,
      metaDataLocal.fromBeginning,
      metaDataLocal.schemaUrl))

  var kafkaStreams: KafkaStreams = null;

  def onMessageReceivedFromTopic(message: Object, metaData: MetaData): Unit = {
    val subscriber = Params.subscriber.get(metaData.consumerGroup)
      .get(metaData.topic)
      .toList(consumerPicker.nextInt(Params.subscriber.get(metaData.consumerGroup)
        .get(metaData.topic)
        .size))
    subscriber.getRemote.sendString(message.toString());
  }

  def connect() {
    var kStreamBuilder: KStreamBuilder = new KStreamBuilder()
    var kstream: KStream[String, String] = kStreamBuilder.stream(Serdes.String(), Serdes.String(), metaDataLocal.topic)
    kstream.mapValues(new ValueMapper[String, String]() {
      @Override
      def apply(value: String): String = {
        onMessageReceivedFromTopic(value, metaDataLocal)
        null
      }
    })

    kafkaStreams = new KafkaStreams(kStreamBuilder, streamConfig)
    kafkaStreams.start()
  }

  def close(): Unit = {
    kafkaStreams.close();
  }

  def getProperties(kafkaUrl: String, zookeeper: String, consumerGroup: String, fromBeginning: Boolean, schemaUrl: String): Properties = {
    var props: Properties = new Properties();
    var processorJob: String = UUID.randomUUID().toString()

    props.put(StreamsConfig.CLIENT_ID_CONFIG, processorJob);
    props.put("group.id", consumerGroup);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, processorJob)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl)
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper)
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")
    //    props.put("num.stream.threads", "3")

    if (fromBeginning) {
      props.put("auto.offset.reset", "earliest")
    } else {
      props.put("auto.offset.reset", "latest")
    }

    props
  }
}