package com.splunk.kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import kafka.utils.ZkUtils

object TaskKafka {
  def main(args: Array[String]): Unit = {
    testProducer
  }

  def testProducer: Unit = {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "t0:9092,t1:9092,t2:9092")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    (1 to 100).foreach((i: Int) => {
      print(".")
      val msg = new KeyedMessage("newTopic", "key", "msg" + i)
      producer.send(msg)
      Thread.sleep(1000)
    })

    producer.close

  }
}
