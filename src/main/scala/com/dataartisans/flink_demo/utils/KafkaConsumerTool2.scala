package com.dataartisans.flink_demo.utils

import java.util
import java.util.{HashMap, List, Map, Properties}
import java.util.concurrent.ExecutorService

import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

class KafkaConsumerTool2 (
                           val server: String,
                           val topic: String,
                           val group: String
                         ){
//  private var consumer: KafkaConsumer[String, String] = null
  private var consumer: ConsumerConnector = null
//  private var topic = null
  private var config: ConsumerConfig = null
  private var executor = null
//  private var consumerMap = null
  def getRecords: KafkaStream[Array[Byte], Array[Byte]] = {
    if (this.consumer == null) {
      this.createConsumerConfig(server, group)
      this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(this.config)


    }
    val topicCountMap = new util.HashMap[String, Integer]
    topicCountMap.put(topic, 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)

    val stream = consumerMap.get(topic).get(0)
    return stream

//    return this.consumer.poll(100)
  }

  private def createConsumerConfig(a_zookeeper: String, a_groupId: String) = {
    val props = new Properties
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    this.config = new ConsumerConfig(props)
  }

}

object KafkaConsumerTool2 {

}
