package com.zqykj.streaming.job

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zqykj.hyjj.entity.elp._
import com.zqykj.hyjj.query.{CompactLinkData, PropertyData}
import com.zqykj.streaming.business.TransformJob._
import com.zqykj.streaming.common.{Contants, ELPOperatorType, MetricContants}
import com.zqykj.streaming.common.Contants._
import com.zqykj.streaming.kafka.KafkaSink
import com.zqykj.streaming.metadata.BussinessStatistics
import com.zqykj.streaming.util.{DistIdUtils, ELPTransUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by alfer on 8/31/17.
  */
class StreamingExecutor(@transient val sc: SparkContext,
                        val kafkaProducer: Broadcast[KafkaSink[String, String]],
                        val elpModelBroadcast: Broadcast[mutable.HashMap[String, ElpModel]],
                        val elpDBMappingBroadcast: Broadcast[mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]],
                        val elpModelAndMappingBroadcast: Broadcast[mutable.HashMap[String, PropertyBag]]) extends Logging with Serializable {

	val elpModelCache = elpModelBroadcast.value
	val elpAndMappingsCache = elpModelAndMappingBroadcast.value
	val operatorType = sc.getConf.get("spark.kafka.stream.transform.elp.operator", ELPOperatorType.ADD.toString)

	// zqykjdev14
	val zkQurom = sc.getConf.get("spark.kafka.stream.transform.zkqurom", "zqykjdev14:2181")

	// bigdataclusterr02:9092,bigdatacluster03:9092,bigdatacluster04:9092,bigdatacluster05:9092,bigdatacluster06:9092
	val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.stream.transform.brokers",
		"zqykjdev14:9092"),
		sparkConf.get("spark.kafka.stream.transform.topics", "DATASCHEMA")) // DATASCHEMA ExtractorRecord
	val topicsSet = topics.split(",").toSet
	println("brokers=" + brokers + " ,topic=" + topics)

	val kafkaParams = Map[String, String](
		"metadata.broker.list" -> brokers,
		"auto.offset.reset" -> sparkConf.get("spark.kafka.stream.transform.auto.offset.reset", "largest"), // largest smallest
		"group.id" -> sparkConf.get("spark.kafka.stream.transform.group.id", "transGroup10"),
		"max.partition.fetch.bytes" -> sparkConf.get("spark.kafka.stream.transform.max.partition.fetch.bytes", "1048576"),
		"connections.max.idle.ms" -> sparkConf.get("spark.kafka.stream.transform.connections.max.idle.ms", "540000"),
		"client.id" -> sparkConf.get("spark.kafka.stream.transform.client.id", "")
	)

	val km = new KafkaManager(kafkaParams)

	// 构造 kafka producer 参数
	val kafkaProParams = Map[String, String](
		"bootstrap.servers" -> sparkConf.get("spark.kafka.elp.data.brokers", "zqykjdev14:9092"),
		"client.id" -> sparkConf.get("spark.kafka.elp.data.client.id", "ElpTransformGroupLocal10"),
		"compression.type" -> sparkConf.get("spark.kafka.elp.data.compression.type", "none"),
		"batch.size" -> sparkConf.get("spark.kafka.elp.data.batch.size", "16384"),
		"max.request.size" -> sparkConf.get("spark.kafka.elp.data.max.request.size", "1048576"),
		"connections.max.idle.ms" -> sparkConf.get("spark.kafka.elp.data.connections.max.idle.ms", "540000"),
		"linger.ms" -> sparkConf.get("spark.kafka.elp.data.linger.ms", "0"),
		"key.serializer" -> sparkConf.get("spark.kafka.elp.data.key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer"),
		"value.serializer" -> sparkConf.get("spark.kafka.elp.data.value.serializer",
			"org.apache.kafka.common.serialization.StringSerializer")
	)
	// loadstandard_model
	val outTopicSet = sparkConf.get("spark.kafka.elp.data.topics", "ELP-standard_model").split(",").toSet
	println(s"outTopicSet: ${outTopicSet}, kafkaProParams: ${kafkaProParams}")
	val metricsTopicSet = sparkConf.get("spark.kafka.elp.data.metric.topics", "Statistics").split(",").toSet

	private def getResetOffsets(): String = {
		// checkout operatorType
		val resetOffsets = if (ELPOperatorType.ADD.toString.equals(operatorType)
			|| ELPOperatorType.MODIFY.equals(operatorType)) {
			"lastOffsets"
		} else {
			sparkConf.get("spark.kafka.stream.transform.auto.offset.reset", "largest")
		}
		resetOffsets
	}

	def receiveFromKafka(ssc: StreamingContext): InputDStream[(String, String)] = {

		val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet, getResetOffsets)
		messages
	}

	def dataTransform(msgs: RDD[(String, String)]): RDD[mutable.HashMap[String, String]] = {

		val msgTransRDD = msgs.mapPartitions(p => {
			val logHashSet = new mutable.HashSet[String]()
			p.map(m => {
				// sequenceid@dataSchemaId@resId@taskId
				val keyArr = m._1.split(Contants.SEQUENCEID_DATASCHEMAID_RESID)
				val dataSchemaId = keyArr(1)
				logDebug(s"key=${m._1}, dataSchemaId=${dataSchemaId}")
				var elpDBMapings: Option[ArrayBuffer[ElpModelDBMapping]] = elpDBMappingBroadcast.value.get(dataSchemaId)
				if (elpDBMapings.isEmpty) {
					// logWarning(s"dataSchemaId: ${dataSchemaId} don't match any elpDBMappings")
					if (!logHashSet.contains(dataSchemaId)) {
						logHashSet.add(dataSchemaId)
						logWarning(s"dataSchemaId: ${dataSchemaId} don't match any elpDBMappings")
					}
				}
				// resId@taskId
				val businessKey = keyArr(2).concat(SEQUENCEID_DATASCHEMAID_RESID).concat(keyArr(3))
				((businessKey, m._2), elpDBMapings)
			}).filter(f =>
				!f._2.isEmpty
			).map(mp => {
				// mp: ((resid, value), elpDBMapping)
				val transData = dbMappingsTransform(mp._1, mp._2.get)
				// logInfo(s"transData count=${transData.size}")
				transData
			})
		})
		msgTransRDD
	}

	/**
	  *
	  * @param data          : (resid@taskid， value)
	  * @param elpDBMappings : ArrayBuffer[ElpModelDBMapping]
	  * @return mutable.HashMap[sequenceId@elp_entity/relation_elpType@resid@taskid, elpValue]
	  */
	def dbMappingsTransform(data: (String, String), elpDBMappings: ArrayBuffer[ElpModelDBMapping])
	: mutable.HashMap[String, String] = {
		val jsonObj = JSON.parseObject(data._2)
		DistIdUtils.apply(zkQurom)
		// ((elpModel, elpType, entity/link, ds),jsonObjString)
		val elMap = new mutable.HashMap[String, String]()
		if (!elpDBMappings.isEmpty) {
			elpDBMappings.foreach(mapping => {
				val key = mapping.getElp + ELP_MAPPING_SEPARATOR + mapping.getElpType + ELP_MAPPING_SEPARATOR + mapping.getElpTypeDesc.toString
				val property = elpAndMappingsCache.get(key)
				if (!property.isEmpty) {
					val sequenceId = DistIdUtils.getSequenceId()
					if (PropertyBag.Type.Entity.equals(mapping.getElpTypeDesc)) {
						val elpEntityData = Option(ELPTransUtils.parseEntity(jsonObj, property.get.asInstanceOf[Entity], mapping))
						// (elpId_entity_elpType, elpEntityData)
						// sequenceId@elp_entity_elpType@resid@taskid
						if (elpEntityData.nonEmpty) {
							elMap.put(sequenceId + SEQUENCEID_DATASCHEMAID_RESID
								+ mapping.getElp + ELP_MAPPING_SEPARATOR + EntityConstants.ELP_ENTITY + ELP_MAPPING_SEPARATOR + mapping.getElpType
								.concat(SEQUENCEID_DATASCHEMAID_RESID).concat(data._1)
								, elpEntityData.get.toJSONString)
						}
					} else if (PropertyBag.Type.Link.equals(mapping.getElpTypeDesc)) {
						// (elpId_relation_elpType, elpLinkData)
						val elpLinkData = Option(ELPTransUtils.parseLink(jsonObj, property.get.asInstanceOf[Link], mapping, elpModelCache.get(mapping.getElp).get))
						if (elpLinkData.nonEmpty) {
							elMap.put(sequenceId + SEQUENCEID_DATASCHEMAID_RESID
								+ mapping.getElp + ELP_MAPPING_SEPARATOR + LinkContants.ELP_LINK + ELP_MAPPING_SEPARATOR + mapping.getElpType
								.concat(SEQUENCEID_DATASCHEMAID_RESID).concat(data._1)
								, elpLinkData.get.toJSONString)
						}
					}
				}
			})
		}
		// DistIdUtils.close()
		elMap
	}

	/**
	  * send to kafka
	  *
	  * @param elpData
	  */
	def sendMsg(elpData: RDD[mutable.HashMap[String, String]]) = {
		elpData.foreachPartition {
			fp => {
				if (fp.nonEmpty) {
					// MyKafkaProducer.setkafkaParams(kafkaProParams)
					fp.foreach(e => {
						val iter = e.iterator
						while (iter.hasNext) {
							val ele = iter.next()
							// (topic, key, value)
							// MyKafkaProducer.send(outTopicSet.head, ele._1, ele._2)
							kafkaProducer.value.send(outTopicSet.head, ele._1, ele._2)
						}
					})
				}
			}
		}
	}

	def compactStatistics(startTime: Date, countBeforeTrans: collection.Map[String, Long], countAfterTrans: collection.Map[String, Long]): Unit = {
		countAfterTrans.foreach(r => {
			val residAndTaskid = r._1.split(SEQUENCEID_DATASCHEMAID_RESID)
			val resid = residAndTaskid(0)
			val taskid = residAndTaskid(1)
			countBeforeTrans.get(resid)
			val statistics = new BussinessStatistics()
			statistics.setMetricName(MetricContants.TransMetrics.REALTIME_BATCH_METRICS_NAME)
			statistics.setTaskId(taskid)
			statistics.setResId(resid)
			statistics.setInTotalRecords(countBeforeTrans.get(r._1).get)
			statistics.setOutTotalRecords(r._2)
			statistics.setElpTypeStatistic(null)
			statistics.setRecordTime(new Date())
			statistics.setStartTime(startTime)
			statistics.setEndTime(null)
			kafkaProducer.value.send(metricsTopicSet.head, MetricContants.TransMetrics.REALTIME_BATCH_METRICS_NAME, statistics.toString)
		})
	}

	/**
	  * Continuous statistics one by one
	  *
	  * @param values
	  * @param runningCount
	  * @return
	  */
	def stateFun(values: Seq[String], runningCount: Option[Long]): Option[Long] = {
		var newValue = if (runningCount.nonEmpty) {
			runningCount.get
		} else {
			0
		}
		val newValueCount = values.size
		newValue += newValueCount
		Option(newValue)
	}

	def execute(): Unit = {
		val ssc = new StreamingContext(sc, Milliseconds(sparkConf.getInt("spark.kafka.transform.batch.millis.duration", 4000)))
		val msg = receiveFromKafka(ssc)
		// record consumed offset of kafka
		var offsetRanges = Array[OffsetRange]()

		// (sequenceid@dataSchemaId@resId@taskId, value)
		val RDDRanges = msg.transform { rdd =>
			offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			rdd
		}

		RDDRanges.foreachRDD { rdd =>
			try {
				val startTime = new Date()
				val taskAndResidCountBeforeTrans = rdd.map(x => {
					val arr = x._1.split(SEQUENCEID_DATASCHEMAID_RESID)
					(arr(2).concat(SEQUENCEID_DATASCHEMAID_RESID).concat(arr(3)), 1l)
				})
					.reduceByKey((x, y) => x + y)
					.collectAsMap()

				// sequenceId@elp_entity_elpType@resid@taskid
				val elpData = dataTransform(rdd)
				sendMsg(elpData)

				val taskidAndResidCountAfterTrans = elpData.map(m => {
					val arr = m.toList(0)._1.split(SEQUENCEID_DATASCHEMAID_RESID)
					(arr(2).concat(SEQUENCEID_DATASCHEMAID_RESID).concat(arr(3)), m.size.toLong)
				})
					.reduceByKey((x, y) => x + y).collectAsMap()

				compactStatistics(startTime, taskAndResidCountBeforeTrans, taskidAndResidCountAfterTrans)

				// update offsets
				for (o <- offsetRanges) {
					km.updateZKOffsets(o)
				}
			} catch {
				case ex: RuntimeException => logError("=============> foreach rdd has RuntimeException", ex)
				case ex: Exception => logError("=============> foreach rdd has Exception", ex)
			}
		}

		ssc.start()
		ssc.awaitTermination()
	}



	def getLabel(label0: String, varNames: java.util.Set[String], varValues: mutable.HashMap[String, String]): String = {
		var label = label0
		try {
			import scala.collection.JavaConversions._
			for (varName <- varNames) {
				label = label.replaceAll("\\$\\{" + varName + "\\}", varValues.get(varName).get)
			}
		} catch {
			case ex: Exception => {
				logError(s"=============> Error when parsing entity label, ${ex}")
				logError(s"=============> Template ${label}, var names: ${varNames}, var values ${varValues}")
			}
		}
		label
	}

}
