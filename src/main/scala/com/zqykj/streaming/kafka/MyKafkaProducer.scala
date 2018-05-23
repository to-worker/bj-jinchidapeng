package com.zqykj.streaming.kafka

import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.Logging

/**
  * Created by weifeng on 2017/6/9.
  */
object MyKafkaProducer extends Serializable with Logging {

    var kafkaParams: Map[String, String] = null

    def setkafkaParams(params: Map[String, String]): Unit = {
        this.kafkaParams = params
    }

    private lazy val producer: KafkaProducer[String, String] = {
        new KafkaProducer[String, String](initProperties())
    }

    def apply(): KafkaProducer[String, String] = producer

    def initProperties(): Properties = {
        println(kafkaParams.seq.mkString(","))
        val props = new Properties
        props.put("bootstrap.servers", kafkaParams.get("bootstrap.servers").get)
        props.put("client.id", kafkaParams.get("client.id").get)
        props.put("compression.type", kafkaParams.get("compression.type").get)
        props.put("batch.size", kafkaParams.get("batch.size").get)
        props.put("max.request.size", kafkaParams.get("max.request.size").get)
        props.put("connections.max.idle.ms", kafkaParams.get("connections.max.idle.ms").get)
        props.put("linger.ms", kafkaParams.get("linger.ms").get)
        props.put("key.serializer", kafkaParams.get("key.serializer").get)
        props.put("value.serializer", kafkaParams.get("value.serializer").get)
        props
    }

    /**
      *
      * @param topic
      * @param key
      * @param value
      * @param isAsync
      */
    def send(topic: String, key: String, value: String, isAsync: Boolean = true): Unit = {
        if (isAsync) { // 异步发送
            producer.send(new ProducerRecord[String, String](topic, key, value), new Callback() {
                override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
                }
            })
        } else { // 同步发送
            try {
                producer.send(new ProducerRecord[String, String](topic, key, value)).get
            } catch {
                case e@(_: InterruptedException | _: ExecutionException) =>
                    logError(s"同步发送异常, key: ${key}, value: ${value}, Exception: ${e}")
            }
        }
    }

}
