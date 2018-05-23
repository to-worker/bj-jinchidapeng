package com.zqykj.streaming.job

import java.util

import com.alibaba.fastjson.JSON
import com.zqykj.streaming.business.BlacklistWarningJob.{sparkConf}
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.{Logging, SparkContext}

/**
  * Created by alfer on 9/16/17.
  */
class BlackWarningStreaming(@transient val sc: SparkContext,
                            val phNumberblacklistBroadCast: Broadcast[util.ArrayList[String]]) extends Logging with Serializable {

    val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.warning.net.brokers",
        "bigdatacluster02:9092,bigdatacluster03:9092,bigdatacluster04:9092,bigdatacluster05:9092,bigdatacluster06:9092"),
        sparkConf.get("spark.kafka.warning.net.in.topics", "ElpLoadData"))
    val topicsSet = topics.split(",").toSet
    println("brokers=" + brokers + " ,topic=" + topics)

    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> brokers,
        "auto.offset.reset" -> sparkConf.get("spark.kafka.warning.net.auto.offset.reset", "largest"),
        "group.id" -> sparkConf.get("spark.kafka.warning.net.client.id", "WarningClientGroup")
    )
    val km = new KafkaManager(kafkaParams)

    def receiveFromKafka(ssc: StreamingContext): InputDStream[(String, String)] = {
        km.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, topicsSet)
    }

    def blackWaringProcess(transformRDD: DStream[(String, String)]): DStream[(String, String)] = {
        transformRDD.filter(f => {
            val josnObj = JSON.parseObject(f._2)
            val ph = josnObj.getJSONObject("body").getString("P2xKYiPZrYCSfPGNw")
            import scala.collection.JavaConversions._
            val blacklist = phNumberblacklistBroadCast.value
            val bufferList = blacklist.filter(black => black.equals(ph))
            if (bufferList.size > 0) {
                true
            } else {
                false
            }
            // TODO BloomFilter
        })
    }

    def execute(): Unit = {
        val ssc = new StreamingContext(sc, Milliseconds(sparkConf.getInt("spark.kafka.warning.net.batch.millis.duration", 2000)))
        val messages = receiveFromKafka(ssc)
        // 记录kafka消费的消息偏移量
        var offsetRanges = Array[OffsetRange]()
        val transformRDD = messages.transform { rdd =>
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        val importMsg = blackWaringProcess(transformRDD)
        importMsg.print()

        transformRDD.foreachRDD {
            rdd => {
                // 更新offsets
                for (o <- offsetRanges) {
                    println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
                    km.updateZKOffsets(o)
                }
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }


}
