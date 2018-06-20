package com.dataartisans.flink_demo.utils

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

class KafkaConsumerTool (
                        val server: String,
                        val topic: String,
                        val group: String
                        ){
  private var consumer: KafkaConsumer[String, String] = null
  def getRecords: ConsumerRecords[String, String] = {
    if (this.consumer == null) {
      val props = new Properties()
      props.put("bootstrap.servers", "host13:9092")
      props.put("group.id", this.group)
      props.put("zookeeper.connect", this.server)
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      this.consumer = new KafkaConsumer(props)
      this.consumer.subscribe(util.Arrays.asList(this.topic))
    }
    return this.consumer.poll(100)
  }

}

object KafkaConsumerTool {

}
