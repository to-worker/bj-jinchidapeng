package com.zqykj.streaming.job

import java.util.{ArrayList, Date, UUID}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lucidworks.spark.SolrSupport
import org.apache.http.impl.client.SystemDefaultHttpClient
import com.zqykj.streaming.util.{DistIdUtils, HGlobalConn, TypeConvertUtils}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.{Logging, SparkContext}
import com.zqykj.streaming.common.Contants._
import com.zqykj.tldw.util.HBaseUtils
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import com.zqykj.streaming.business.LoadJob._
import com.zqykj.streaming.common.MetricContants.LoaderMetrics
import com.zqykj.streaming.common.{Contants, ELPOperatorType, MetricContants}
import com.zqykj.streaming.kafka.KafkaSink
import com.zqykj.streaming.metadata.{BussinessStatistics, ElpTypeStatistics}
import com.zqykj.streaming.solr.{SolrClient, SolrClientSupport}
import org.apache.hadoop.hbase.{RegionTooBusyException, TableName}
import org.apache.http.client.HttpClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by alfer on 9/12/17.
  */
class StreamPersistence(@transient val sc: SparkContext,
                        val kafkaProducer: Broadcast[KafkaSink[String, String]],
                        val filteredELPBroadcast: Broadcast[mutable.HashSet[String]],
                        val datePatternBroadcast: Broadcast[mutable.HashMap[String, java.util.List[String]]])
	extends Logging with Serializable {

	val operatorType = sc.getConf.get("spark.kafka.elp.load.operator", ELPOperatorType.ADD.toString)
	val filteredELP = filteredELPBroadcast.value
	val datePatterns = datePatternBroadcast.value

	val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.elp.load.brokers",
		"zqykjdev14:9092"),
		sparkConf.get("spark.kafka.elp.load.topics", "ELP-standard_model")) // loadstandard_model
	val topicsSet = topics.split(",").toSet
	println("brokers=" + brokers + " ,topic=" + topics)
	val metricsTopicSet = sparkConf.get("spark.kafka.elp.data.metric.topics", "Statistics").split(",").toSet

	val kafkaParams = Map[String, String](
		"metadata.broker.list" -> brokers,
		// largest
		"auto.offset.reset" -> sparkConf.get("spark.kafka.elp.load.auto.offset.reset", "largest"),
		"group.id" -> sparkConf.get("spark.kafka.elp.load.group.id", "ElpLoadClientGroup10"), //  clientTestLocal2
		"max.partition.fetch.bytes" -> sparkConf.get("spark.kafka.elp.load.max.partition.fetch.bytes", "1048576"),
		"connections.max.idle.ms" -> sparkConf.get("spark.kafka.elp.load.connections.max.idle.ms", "540000"),
		"client.id" -> sparkConf.get("spark.kafka.elp.load.client.id", "")
	)

	val solrConfig = sparkConf.get("spark.load.solr.zk.server", "zqykjdev14:2181/solrv7test")
	val solrZKQuorum = sparkConf.get("spark.load.solr.zk.host", "zqykjdev14")
	val solrZkChroot = sparkConf.get("spark.load.solr.zk.chroot", "/solrv7test")
	val hbaseZKQuorum = sparkConf.get("spark.load.hbase.zk.quorum", "zqykjdev14")
	val km = new KafkaManager(kafkaParams)
	val batchSize = sparkConf.getInt("spark.trans.solr.batch.size", 20000)
	val batchSize2HBase = sparkConf.getInt("spark.trans.hbase.batch.size", 20000)

	/**
	  * (ADD:增加实体或链接 or MODIFY:修改实体或者链接) -> lastOffsets -> offset更新最新
	  * OTHER -> largest -> 从实际位置开始消费
	  * OTHER -> smallest -> 从最小offset开始消费
	  *
	  * @return
	  */
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

	def putBody(jsonObj: JSONObject, put: Put): Put = {
		val jsonBody = jsonObj.getJSONObject("body")
		val bodyIter = jsonBody.entrySet().iterator()
		while (bodyIter.hasNext) {
			val element = bodyIter.next()
			val key = element.getKey
			val value = element.getValue.toString
			// val kv = convertType(key, value)
			put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
				Bytes.toBytes(key),
				Bytes.toBytes(JSON.parseObject(value).get("value").toString))
		}
		put
	}

	def convertPutType(key: String, value: String): (String, Object) = {
		val valueAndType = JSON.parseObject(value)
		val fieldType = valueAndType.get("type")
		val fieldValue: String = valueAndType.get("value").toString

		val kv = fieldType match {
			case PropertyTypeConstants.date => (key, TypeConvertUtils.getTypeConvert("date", fieldValue))
			case PropertyTypeConstants.datetime => (key, TypeConvertUtils.getTypeConvert("datetime", fieldValue))
			case PropertyTypeConstants.time => (key, fieldValue)
			case _ => {
				(key, value)
			}
		}
		kv
	}


	def compactEntityPut(jsonObj: JSONObject): Put = {
		val put = new Put(Bytes.toBytes(HBaseUtils.md5Hash(jsonObj.getString(EntityConstants.HBASE_TABLE_ROWKEY))))
		put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(EntityConstants.VERTEX_ID_FILED),
			Bytes.toBytes(jsonObj.getString(EntityConstants.VERTEX_ID_FILED)))
		put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(EntityConstants.VERTEX_TYPE_FILED),
			Bytes.toBytes(jsonObj.getString(EntityConstants.VERTEX_TYPE_FILED)))
		put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(EntityConstants.VERTEXT_RESID),
			Bytes.toBytes(jsonObj.getString(EntityConstants.VERTEXT_DSID)))
		put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(EntityConstants.VERTEXT_OWNER),
			Bytes.toBytes(jsonObj.getString(EntityConstants.VERTEXT_OWNER)))

		putBody(jsonObj, put)
	}

	def compactLinkPut(jsonObj: JSONObject): Put = {
		val put = new Put(Bytes.toBytes(HBaseUtils.md5Hash(jsonObj.getString(LinkContants.HBASE_TABLE_ROWKEY))))
		put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD)))
		put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_FROM_VERTEX_ID_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_FROM_VERTEX_ID_FIELD)))
		put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD)))
		put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_TO_VERTEX_ID_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_TO_VERTEX_ID_FIELD)))
		put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_DIRECTION_TYPE_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_DIRECTION_TYPE_FIELD)))

		put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_TYPE_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_TYPE_FIELD)))
		put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_ID_FIELD),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_ID_FIELD)))
		put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_RESID),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_DSID)))
		put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
			Bytes.toBytes(LinkContants.EDGE_OWNER),
			Bytes.toBytes(jsonObj.getString(LinkContants.EDGE_OWNER)))

		putBody(jsonObj, put)
	}

	def docBody(jsonObj: JSONObject, solrDocument: SolrInputDocument): SolrInputDocument = {
		val jsonBody = jsonObj.getJSONObject("body")
		val bodyIter = jsonBody.entrySet().iterator()
		while (bodyIter.hasNext) {
			val element = bodyIter.next()
			val key = element.getKey
			val value = element.getValue.toString
			val fieldValue = convertType(key, value)
			if (!"".equals(fieldValue._1)) {
				solrDocument.setField(fieldValue._1, fieldValue._2)
			}
		}
		solrDocument
	}

	/**
	  * convert to solr data format according to elp  data format
	  *
	  * @param key   : field name
	  * @param value : field value
	  * @return
	  */
	def convertType(key: String, value: String): (String, Object) = {
		val valueAndType = JSON.parseObject(value)
		val fieldType = valueAndType.get("type")
		val fieldValue: String = valueAndType.get("value").toString.trim

		val kv = fieldType match {
			case PropertyTypeConstants.bool => ("g_bn11_".concat(key), fieldValue)
			case PropertyTypeConstants.date => ("g_rd11_".concat(key), TypeConvertUtils.getTypeConvert(PropertyTypeConstants.date, fieldValue,
				datePatterns.get(PropertyTypeConstants.date).get))
			case PropertyTypeConstants.datetime => ("g_rd11_".concat(key), TypeConvertUtils.getTypeConvert(PropertyTypeConstants.datetime,
				fieldValue, datePatterns.get(PropertyTypeConstants.datetime).get))
			case PropertyTypeConstants.time => ("g_tk11_".concat(key), TypeConvertUtils.getTypeConvert(PropertyTypeConstants.time,
				fieldValue, datePatterns.get(PropertyTypeConstants.time).get))
			case PropertyTypeConstants.integer => ("g_it11_".concat(key), fieldValue)
			case PropertyTypeConstants.number => ("g_de11_".concat(key), TypeConvertUtils.getTypeConvert("number", fieldValue))
			case PropertyTypeConstants.text => ("g_tt11_".concat(key), fieldValue)
			case PropertyTypeConstants.string => ("g_tk11_".concat(key), fieldValue)
			case _ => {
				logWarning(s"=============> key: ${key}, fieldType: ${fieldType}, fieldValue: ${fieldValue} 没有匹配到任何类型")
				("", "")
			}
		}
		kv
	}

	def compactEntityDoc(jsonObj: JSONObject): SolrInputDocument = {
		val solrDocument = new SolrInputDocument()
		solrDocument.setField(EntityConstants.HBASE_TABLE_ID, HBaseUtils.md5Hash(jsonObj.getString(EntityConstants.HBASE_TABLE_ROWKEY)))
		solrDocument.setField(EntityConstants.VERTEX_ID_FILED, jsonObj.getString(EntityConstants.VERTEX_ID_FILED))
		solrDocument.setField(EntityConstants.VERTEX_TYPE_FILED, jsonObj.getString(EntityConstants.VERTEX_TYPE_FILED))
		solrDocument.setField(EntityConstants.VERTEXT_RESID, jsonObj.getString(EntityConstants.VERTEXT_DSID))
		solrDocument.setField(EntityConstants.VERTEXT_OWNER, jsonObj.getString(EntityConstants.VERTEXT_OWNER))
		docBody(jsonObj, solrDocument)
	}

	def compactLinkDoc(jsonObj: JSONObject): SolrInputDocument = {
		val solrDocument = new SolrInputDocument()
		solrDocument.setField(LinkContants.HBASE_TABLE_ID, HBaseUtils.md5Hash(jsonObj.getString(LinkContants.HBASE_TABLE_ROWKEY)))
		solrDocument.setField(LinkContants.EDGE_ID_FIELD, jsonObj.getString(LinkContants.EDGE_ID_FIELD))
		solrDocument.setField(LinkContants.EDGE_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_TYPE_FIELD))
		solrDocument.setField(LinkContants.EDGE_DIRECTION_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_DIRECTION_TYPE_FIELD))
		solrDocument.setField(LinkContants.EDGE_FROM_VERTEX_ID_FIELD, jsonObj.getString(LinkContants.EDGE_FROM_VERTEX_ID_FIELD))
		solrDocument.setField(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD))
		solrDocument.setField(LinkContants.EDGE_TO_VERTEX_ID_FIELD, jsonObj.getString(LinkContants.EDGE_TO_VERTEX_ID_FIELD))
		solrDocument.setField(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD))
		solrDocument.setField(LinkContants.EDGE_RESID, jsonObj.getString(LinkContants.EDGE_DSID))
		solrDocument.setField(LinkContants.EDGE_OWNER, jsonObj.getString(LinkContants.EDGE_OWNER))

		docBody(jsonObj, solrDocument)
	}

	/**
	  * (elp_relatoin_elpType, elpData)
	  * (elp_entity_elpType, elpData)
	  *
	  * @param keyAndValues
	  * @param offsetRanges
	  */
	def persist(keyAndValues: DStream[(String, String)], offsetRanges: Array[OffsetRange]) = {
		keyAndValues.foreachRDD(rdd => {
			var msg: String = ""
			try {
				rdd.foreachPartition(fp => {
					val gMap = new mutable.HashMap[String, mutable.HashSet[String]]()
					fp.foreach(f => {
						if (!gMap.contains(f._1)) {
							val hashSet = new mutable.HashSet[String]()
							hashSet.add(f._2)
							gMap.put(f._1, hashSet)
						} else {
							gMap.get(f._1).get.add(f._2)
						}
					})
					logInfo(s"total ${gMap.size} data prepared to persist")
					if (gMap.size > 0) {
						gMap.foreach(f => {
							// val endCountDown = new CountDownLatch(2)
							val pool = Executors.newFixedThreadPool(2)
							try {
								val solrFuture = pool.submit(new Callable[Long] {
									override def call(): Long = {
										val flag = solrPersist(f)
										// endCountDown.countDown()
										flag
									}
								})

								val hbaseFuture = pool.submit(new Callable[Long] {
									override def call(): Long = {
										val flag = hbasePersist(f)
										// endCountDown.countDown()
										flag
									}
								})

								// endCountDown.await()
							} catch {
								case ex: InterruptedException => {
									logError("endCountDown interrupted.", ex)
									throw new RuntimeException(s"writing solr or hbase occur to some thread exception: ${ex.getStackTraceString}", ex)
								}
								case ex: Exception => {
									logError(s"${ex.getStackTraceString}", ex)
									throw new Exception(s"writing solr or hbase occur to some exception: ${ex.getStackTraceString}", ex)
								}
							} finally {
								pool.awaitTermination(1L, TimeUnit.SECONDS)
								pool.shutdown()
							}

						})
					}
				})
			} catch {
				case ex: Exception => {
					msg = s"persist occur to exception: ${ex.getStackTraceString}"
					logError(msg, ex)
				}
			}
		})
	}

	def persist(rdd: RDD[(String, String)]) = {
		var msg: String = ""
		try {
			rdd.foreachPartition(fp => {
				val gMap = new mutable.HashMap[String, mutable.HashSet[String]]()
				fp.foreach(f => {
					if (!gMap.contains(f._1)) {
						val hashSet = new mutable.HashSet[String]()
						hashSet.add(f._2)
						gMap.put(f._1, hashSet)
					} else {
						gMap.get(f._1).get.add(f._2)
					}
				})
				logInfo(s"total ${gMap.size} data prepared to persist")
				if (gMap.size > 0) {
					gMap.foreach(f => {
						// val endCountDown = new CountDownLatch(2)
						val pool = Executors.newFixedThreadPool(2)
						try {
							val solrFuture = pool.submit(new Callable[Long] {
								override def call(): Long = {
									val count = solrPersist(f)
									// endCountDown.countDown()
									count
								}
							})

							val hbaseFuture = pool.submit(new Callable[Long] {
								override def call(): Long = {
									val count = hbasePersist(f)
									// endCountDown.countDown()
									count
								}
							})

							logInfo(s"write ${solrFuture.get()} data to solr, write ${hbaseFuture.get()} data to hbase.")
							// endCountDown.await()
						} catch {
							case ex: InterruptedException => {
								logError("endCountDown interrupted.", ex)
								throw new RuntimeException(s"writing solr or hbase occur to some thread exception: ${ex.getStackTraceString}", ex)
							}
							case ex: Exception => {
								logError(s"${ex.getStackTraceString}", ex)
								throw new Exception(s"writing solr or hbase occur to some exception: ${ex.getStackTraceString}", ex)
							}
						} finally {
							pool.awaitTermination(1L, TimeUnit.SECONDS)
							pool.shutdown()
						}

					})
				}
			})
		} catch {
			case ex: Exception => {
				msg = s"persist occur to exception: ${ex.getStackTraceString}"
				logError(msg, ex)
			}
		}
	}


	/**
	  *
	  * @param f
	  */
	def hbasePersist(f: (String, Iterable[String])): Long = {
		var hbaseCount = 0l
		val datas = f._2.iterator
		if (datas.nonEmpty) {
			val hConnection = ConnectionFactory.createConnection(HGlobalConn.getConfiguration(hbaseZKQuorum))
			val mutateBuffer = hConnection.getBufferedMutator(TableName.valueOf(f._1))
			val putList = new ArrayList[Put]()

			try {
				var putDatas: Iterator[Put] = null
				if (datas.nonEmpty) {
					if (f._1.contains(EntityConstants.ELP_ENTITY)) {
						putDatas = datas.map(data => {
							val jsonObj = JSON.parseObject(data)
							compactEntityPut(jsonObj)
						})
					} else {
						putDatas = datas.map(data => {
							val jsonObj = JSON.parseObject(data)
							compactLinkPut(jsonObj)
						})
					}
				}

				if (Option(putDatas).nonEmpty) {
					putDatas.foreach(put => {
						putList.add(put)
						if (putList.size() > batchSize2HBase) {
							logInfo(s"write ${putList.size} puts to ${f._1} hbase table.")
							hbaseCount += putList.size()
							mutateBuffer.mutate(putList)
							putList.clear()
						}
					})
					if (putList.size() > 0) {
						logInfo(s"write ${putList.size} puts to ${f._1} hbase table.")
						hbaseCount += putList.size()
						mutateBuffer.mutate(putList)
						putList.clear()
					}
				}
			} catch {
				case ex: RegionTooBusyException => logError(s"write to HBase occur RegionTooBusyException: ${ex.getStackTraceString}", ex)
				case ex: Exception => logError(s"write to HBase occur Exception: ${ex.getStackTraceString}")
			} finally {

				if (Option(mutateBuffer).nonEmpty) {
					mutateBuffer.close()
				}
				if (Option(hConnection).nonEmpty) {
					hConnection.close()
				}
			}
		}
		hbaseCount
	}

	/**
	  *
	  * @param f
	  * @return
	  */
	def solrPersist(f: (String, Iterable[String])): Long = {
		var persistCount = 0l
		val datas = f._2.iterator
		if (datas.nonEmpty) {
			// logInfo(s"perpare writing ${datas.size} data to compact solrInputDocument")
			val elpIdAndType = f._1
			val isEntity = if (elpIdAndType.contains(EntityConstants.ELP_ENTITY)) true else false

			var collectionName = "test14_1"
			if (isEntity) {
				collectionName = elpIdAndType.substring(0, elpIdAndType.indexOf(EntityConstants.SOLR_COLLECTION_SEPERATOR)).concat(EntityConstants.SOLR_COLLECTION_SUFFIX)
			} else {
				collectionName = elpIdAndType.substring(0, elpIdAndType.indexOf(LinkContants.SOLR_COLLECTION_SEPERATOR)).concat(LinkContants.SOLR_COLLECTION_SUFFIX)
			}

			// val httpClient: HttpClient = new SystemDefaultHttpClient()
			val solrClient = new SolrClient(solrZKQuorum, solrZkChroot, collectionName)
			val collectionDocs = new ArrayList[SolrInputDocument]()
			try {
				val docDatas = datas.map(data => {
					val jsonObj = JSON.parseObject(data)
					if (isEntity) {
						compactEntityDoc(jsonObj)
					} else {
						compactLinkDoc(jsonObj)
					}
				})

				val indexedAt = new Date()
				for (inputDoc <- docDatas) {
					inputDoc.setField("_indexed_at_tdt", indexedAt)
					collectionDocs.add(inputDoc)

					if (collectionDocs.size >= batchSize) {
						logInfo(s"write ${collectionDocs.size} data to ${collectionName} collection of solr.")
						persistCount += collectionDocs.size()
						SolrClientSupport.sendBatchToSolr(solrClient.getCloudSolrClient, collectionName, collectionDocs)
					}
				}
				if (!collectionDocs.isEmpty) {
					logInfo(s"write ${collectionDocs.size} data to ${collectionName} collection of solr.")
					persistCount += collectionDocs.size()
					SolrClientSupport.sendBatchToSolr(solrClient.getCloudSolrClient, collectionName, collectionDocs)
				}
			} catch {
				case ex: Exception => logError("=============> occur to some error during writing to solr ", ex)
			} finally {
				solrClient.close()
			}
		}
		persistCount
	}

	/**
	  *
	  * @param messages : (sequenceId@elp_relatoin_elpType@resid@taskid, elpData)
	  * @return
	  */
	def splitMsg(messages: DStream[(String, String)]): DStream[(Long, String, String, String)] = {
		val spMsgs = messages.map(msg => {
			val arr = msg._1.split(SEQUENCEID_DATASCHEMAID_RESID)
			(arr(0).toLong, arr(1), msg._2, arr(2).concat(SEQUENCEID_DATASCHEMAID_RESID).concat(arr(3)))
		})
		spMsgs
	}

	def filterMsgs(spMsgs: DStream[(Long, String, String, String)]): DStream[(Long, String, String, String)] = {
		val filteredMsgs = if (filteredELP.nonEmpty) {
			val newSequenceId = sparkConf.getLong("load.sequence.id", 0)
			spMsgs.filter(msg => {
				if (!filteredELP.contains(msg._2) && DistIdUtils.compare(msg._1, newSequenceId)) {
					true
				} else {
					false
				}
			})
		} else {
			spMsgs
		}
		filteredMsgs
	}


	/**
	  *
	  * @param startTime
	  * @param residAndElpTypeStatistics : resid@taskid@elp_relatoin_elpType
	  */
	def compactMetrics(startTime: Date, residAndElpTypeStatistics: Array[(String, Long)]): Unit = {
		val residMap = new mutable.HashMap[String, java.util.List[ElpTypeStatistics]]()
		residAndElpTypeStatistics.foreach(re => {
			logInfo(s"residAndElpTypeStatistics: ${re}")
			val arr = re._1.split(SEQUENCEID_DATASCHEMAID_RESID)
			val resid = arr(0)
			val taskid = arr(1)
			val elpType = arr(2)
			val elpTypeStatistics = new ElpTypeStatistics()
			elpTypeStatistics.setElpType(elpType)
			elpTypeStatistics.setCount(re._2)
			val residAndTaskid = resid.concat(SEQUENCEID_DATASCHEMAID_RESID).concat(taskid)
			if (!residMap.contains(residAndTaskid)) {
				val list = new java.util.ArrayList[ElpTypeStatistics]()
				list.add(elpTypeStatistics)
				residMap.put(residAndTaskid, list)
			} else {
				residMap.get(residAndTaskid).get.add(elpTypeStatistics)
			}
		})

		residMap.foreach(rm => {
			val arr = rm._1.split(SEQUENCEID_DATASCHEMAID_RESID)
			val resid = arr(0)
			val taskid = arr(1)
			val bussinessStatistics = new BussinessStatistics()
			bussinessStatistics.setMetricName(MetricContants.LoaderMetrics.REALTIME_BATCH_METRIC_NAME)
			bussinessStatistics.setTaskId(taskid)
			bussinessStatistics.setResId(resid)
			bussinessStatistics.setElpTypeStatistic(rm._2)
			var sum: Long = 0
			rm._2.asScala.foreach(f => sum += f.getCount)
			bussinessStatistics.setInTotalRecords(sum)
			bussinessStatistics.setOutTotalRecords(sum)
			bussinessStatistics.setRecordTime(new Date())
			bussinessStatistics.setStartTime(startTime)
			bussinessStatistics.setEndTime(null)
			kafkaProducer.value.send(metricsTopicSet.head, LoaderMetrics.REALTIME_BATCH_METRIC_NAME, bussinessStatistics.toString)
		})
	}

	def execute(): Unit = {

		val ssc = new StreamingContext(sc, Milliseconds(sparkConf.getInt("spark.kafka.elp.load.batch.millis.duration", 4000)))
		val messages = receiveFromKafka(ssc)
		// (key, Iterator[(key,value)])
		// 记录kafka消费的消息偏移量
		var offsetRanges = Array[OffsetRange]()
		val keyAndValues = messages.transform {
			rdd =>
				offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
				rdd
		}
		val spMsgs = splitMsg(keyAndValues)
		val fMsgs = filterMsgs(spMsgs)

		/**
		  * (elp_relatoin_elpType, elpData, resid@taskid)
		  */
		val persisValues = fMsgs.map(m => (m._2, m._3, m._4))

		persisValues.foreachRDD {
			rdd => {
				val startTime = new Date()
				// resid@taskid@elp_relatoin_elpType
				val residAndElpTypeStatistics = rdd.map(m => ((m._3.concat(SEQUENCEID_DATASCHEMAID_RESID).concat(m._1)), 1l))
					.reduceByKey((x, y) => x + y)
					.collect()

				compactMetrics(startTime, residAndElpTypeStatistics)

				persist(rdd.map(m => (m._1, m._2)))
				// 更新offsets
				for (o <- offsetRanges) {
					km.updateZKOffsets(o)
				}
			}
		}

		ssc.start()
		ssc.awaitTermination()
	}


}