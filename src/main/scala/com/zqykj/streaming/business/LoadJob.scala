package com.zqykj.streaming.business

import java.util.Properties

import com.zqykj.streaming.job.{SqlExecutor, StreamPersistence}
import com.zqykj.streaming.kafka.KafkaSink
import com.zqykj.streaming.util.LoggerLevels
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by alfer on 9/12/17.
  */
object LoadJob extends Logging {

	LoggerLevels.setStreamingLogLevels(Level.INFO)

	val sparkConf = new SparkConf()
		.setAppName("Load ELP data to persist")
		.set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "512m")
		.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")

	if (sparkConf.getBoolean("spark.execute.local.model", true))
		sparkConf.setMaster("local[4]").set("spark.ui.port", "4041")
			.set("spark.streaming.kafka.maxRatePerPartition", "1000")

	val options = Map[String, String](
		"host" -> sparkConf.get("spark.streaming.mongodb.host", "zqykjdev14"),
		"database" -> sparkConf.get("spark.streaming.mongodb.db", "hyjj"),
		"user" -> sparkConf.get("spark.streaming.mongodb.db.user", "zqykj"),
		"password" -> sparkConf.get("spark.streaming.mongodb.db.password", "zqykj")
	)

	def main(args: Array[String]): Unit = {

		val sc = new SparkContext(sparkConf)

		// 广播KafkaSink
		val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
			val kafkaProducerConfig = {
				val p = new Properties()
				p.setProperty("bootstrap.servers", sparkConf.get("spark.kafka.elp.load.brokers", "zqykjdev14:9092"))
				p.setProperty("key.serializer", sparkConf.get("spark.kafka.elp.load.key.serializer",
					"org.apache.kafka.common.serialization.StringSerializer"))
				p.setProperty("value.serializer", sparkConf.get("spark.kafka.elp.load.value.serializer",
					"org.apache.kafka.common.serialization.StringSerializer")
				)
				p
			}
			log.warn("kafka producer init done!")
			sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
		}


		val sqlExecutor = new SqlExecutor(sc)
		val datePatternMap = sqlExecutor.cacheSystemPatternDateConfig(sqlExecutor.loadData("SystemParameter"))
		val datePatternBroadcast = sc.broadcast(datePatternMap)

		val filteredELP = new mutable.HashSet[String]()
		val filteredBroadcast = sc.broadcast(filteredELP)
		val streamPersistence = new StreamPersistence(sc, kafkaProducer, filteredBroadcast, datePatternBroadcast)
		streamPersistence.execute()

	}

}
