package com.zqykj.streaming.business

import java.util.ArrayList
import java.util.Properties

import com.zqykj.streaming.common.Contants
import com.zqykj.streaming.job.{SqlExecutor, StreamingExecutor}
import com.zqykj.streaming.util.{DistIdUtils, ELPTransUtils, LoggerLevels}
import org.apache.log4j.Level
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.zqykj.streaming.common.Contants._
import com.zqykj.streaming.kafka.KafkaSink
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConverters._


/**
  * Created by alfer on 8/29/17.
  */
object TransformJob extends Logging {

	LoggerLevels.setStreamingLogLevels(Level.INFO)

	val sparkConf = new SparkConf()
		.setAppName("ELP transform")
		.set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "512m")
		.set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")

	if (sparkConf.getBoolean("spark.execute.local.model", true)) sparkConf.setMaster("local[4]")

	val options = Map[String, String](
		"host" -> sparkConf.get("spark.streaming.mongodb.host", "zqykjdev14"),
		"database" -> sparkConf.get("spark.streaming.mongodb.db", "hyjj"),
		"user" -> sparkConf.get("spark.streaming.mongodb.db.user", "zqykj"),
		"password" -> sparkConf.get("spark.streaming.mongodb.db.password", "zqykj")
	)

	def consumerKafka(): Long = {
		val properties = new Properties()
		properties.setProperty("metadata.broker.list", sparkConf.get("spark.kafka.stream.transform.brokers",
			"zqykjdev14:9092"))
		properties.setProperty("auto.offset.reset", sparkConf.get("spark.kafka.stream.transform.auto.offset.reset", "largest"))
		properties.setProperty("group.id", sparkConf.get("spark.kafka.stream.transform.group.id", "client11"))
		val kafkaConsumer = new KafkaConsumer[String, String](properties)
		kafkaConsumer.subscribe(java.util.Arrays.asList(sparkConf.get("spark.kafka.stream.transform.topics", "DATASCHEMA")))
		val consumerRecords = kafkaConsumer.poll(Long.MaxValue)
		val topicPartitions = consumerRecords.partitions()
		val list = new ArrayList[Long]()
		for (partition: TopicPartition <- topicPartitions.asScala) {
			val partitionRecords = consumerRecords.records(partition)
			if (partitionRecords.size() > 0) {
				val record = partitionRecords.get(0)
				println(s"offset: ${record}, key: ${record.key()}, value: ${record.value()}")
				if (record.key().nonEmpty) {
					val sequenceId = record.key().split(Contants.SEQUENCEID_DATASCHEMAID_RESID)(0)
					list.add(sequenceId.toLong)
				}
			}
		}
		//val sList = list.toArray.toList.sortBy(s => s)
		//		list.get(0)
		//		sList.head
		0
	}

	def main(args: Array[String]): Unit = {

		val sc = new SparkContext(sparkConf)
		// consumerKafka()

		ELPTransUtils.getfilteredELP(sparkConf, options)

		// 广播KafkaSink
		val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
			val kafkaProducerConfig = {
				val p = new Properties()
				p.setProperty("bootstrap.servers", sparkConf.get("spark.kafka.elp.data.brokers", "zqykjdev14:9092"))
				p.setProperty("key.serializer", sparkConf.get("spark.kafka.elp.data.key.serializer",
					"org.apache.kafka.common.serialization.StringSerializer"))
				p.setProperty("value.serializer", sparkConf.get("spark.kafka.elp.data.value.serializer",
					"org.apache.kafka.common.serialization.StringSerializer")
				)
				p
			}
			log.warn("kafka producer init done!")
			sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
		}

		val sqlExecutor = new SqlExecutor(sc)
		val elpModelCache = sqlExecutor.cacheElpModels(sqlExecutor.loadData(ELP_MODELS))
		val elpModelBroadcast = sc.broadcast(elpModelCache)
		val elpDBMappingBroadcast = sc.broadcast(sqlExecutor.cacheElpDBMappings(sqlExecutor.loadData(ELP_MODEL_DB_MAPPING)))

		val elpModelAndMappingsBroadcast = sc.broadcast(sqlExecutor.cacheElpEntitiesAndLinks(elpModelCache))
		val streamingExecutor = new StreamingExecutor(sc, kafkaProducer, elpModelBroadcast, elpDBMappingBroadcast, elpModelAndMappingsBroadcast)
		streamingExecutor.execute()
	}

}
