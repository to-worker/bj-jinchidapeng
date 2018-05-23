package com.zqykj.batch.document.job

import java.util
import java.util.{Date, List}

import com.alibaba.fastjson.JSON
import com.zqykj.batch.document.common.TaskElementStatus
import com.zqykj.batch.document.db.{DocMongoExecutor, HBaseExecutor, MongoUtils, SolrExecutor}
import com.zqykj.batch.document.entity.{DocCardExtractorRule, DocTaskType, DocType, PropertyMapping}
import com.zqykj.streaming.common.Contants
import com.zqykj.streaming.dao.LoadMongoDao
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * @author alfer at 12/21/17
  */
object DocDataCollectJob extends Logging {

	val taskHistoryCollection = "DocExtractorTaskHistory"

	def checkArgs(args: Array[String]): Unit = {
		if (args.length < 1) {
			logError("args error")
			throw new IllegalArgumentException("args error, need:[docType]")
		}
	}

	/**
	  * (
	  * elpModel,
	  * (originalElpType, key, ArrayBuffer((sP, dP))),
	  * ArrayBuffer((dP, targetEntityType, sPId, sP)),
	  * ArrayBuffer((dP, linkedRelationType, sPId ,sP)),
	  * ArrayBuffer((dPLevel1, extensionType, sPId, sPLevel1), (dPLevel2, extensionType, sPIdLevel2, sPLevel2))
	  * )
	  *
	  * @param rule
	  */
	def orderRule(rule: DocCardExtractorRule): (String, (String, String, ArrayBuffer[(String, String)]),
		ArrayBuffer[(String, String, String, String)],
		ArrayBuffer[(String, String, String, String)],
		ArrayBuffer[((String, String, String, String), ArrayBuffer[(String, String, String, String)])]) = {
		val elpModelId = rule.getElpModel

		var originalProps = (rule.getElpType, rule.getKey, ArrayBuffer[(String, String)]())
		var targetRelProps = ArrayBuffer[(String, String, String, String)]()
		var targetEntityProps = ArrayBuffer[(String, String, String, String)]()
		var extensionProps = ArrayBuffer[((String, String, String, String), ArrayBuffer[(String, String, String, String)])]()

		getRule(rule.getPropertyMappings, originalProps, targetEntityProps, targetRelProps, extensionProps)
		getRule(rule.getIndexPropertyMappings, originalProps, targetEntityProps, targetRelProps, extensionProps)

		logInfo(s"originalProps ============> ${originalProps}")
		logInfo(s"targetEntityProps ============>  ${targetEntityProps}")
		logInfo(s"targetRelProps ============>  ${targetRelProps}")
		(elpModelId, originalProps, targetEntityProps, targetRelProps, extensionProps)
	}

	def getRule(ruleMapping: List[PropertyMapping], originalProps: (String, String, ArrayBuffer[(String, String)])
	            , targetEntityProps: ArrayBuffer[(String, String, String, String)]
	            , targetRelProps: ArrayBuffer[(String, String, String, String)]
	            , extensionProps: ArrayBuffer[((String, String, String, String), ArrayBuffer[(String, String, String, String)])]): Unit = {
		if (!ruleMapping.isEmpty) {
			for (pm: PropertyMapping <- ruleMapping.asScala) {
				println(s"isNeedExtendsion= ${pm.isNeedExtendsion}")
				if (pm.isNeedExtendsion) {
					val levelFirst = pm.getSourceTargetEntities.get(0)
					if (levelFirst.isOnlyFromRel) {
						if (Option(levelFirst.getLinkedRelationType).nonEmpty) {
							if (levelFirst.getsPropertyId().nonEmpty && levelFirst.getsPropertyName().nonEmpty) {
								val levelFirstTuple = (levelFirst.getdPropertyName(), levelFirst.getLinkedRelationType, levelFirst.getsPropertyId(), levelFirst.getsPropertyName())
								val secLevelEles = levelFirst.getSecondLevel
								val secondProps = ArrayBuffer[(String, String, String, String)]()
								for (ele <- secLevelEles.asScala) {
									val secLevelTuple = (ele.getdPropertyName(), ele.getTargetEntityType, ele.getsPropertyId(), ele.getsPropertyName())
									secondProps += secLevelTuple
								}
								val dlsp = (levelFirstTuple, secondProps)
								extensionProps += dlsp
							} else {
								logWarning(s"${pm.getdPropertyName()} has sPropertyId pr sPropertyName is empty ")
							}
						}
					}
				} else {
					if (!pm.isNeedRelation) {
						if (!pm.getsPropertyName().isEmpty) {
							val sdps = (pm.getsPropertyName(), pm.getdPropertyName())
							originalProps._3 += sdps
						} else {
							logWarning(s"${pm.getdPropertyName()} has sPropertyId pr sPropertyName is empty ")
						}
					} else if (pm.isNeedRelation) {
						val relElp = pm.getSourceTargetEntities.get(0)
						if (relElp.isOnlyFromRel) {
							if (relElp.getLinkedRelationType.nonEmpty) {
								if (relElp.getsPropertyId().nonEmpty && relElp.getsPropertyName().nonEmpty) {
									val dlsp = (pm.getdPropertyName(), relElp.getLinkedRelationType, relElp.getsPropertyId(), relElp.getsPropertyName())
									targetRelProps += dlsp
								} else {
									logWarning(s"${pm.getdPropertyName()} has sPropertyId pr sPropertyName is empty ")
								}
							} else {
								logWarning(s"${pm.getdPropertyName()} from relation, but relation is null")
							}
						} else if (!relElp.isOnlyFromRel) {
							val targetElp = pm.getSourceTargetEntities.get(0)
							if (targetElp.getTargetEntityType.nonEmpty) {
								if (targetElp.getsPropertyId().nonEmpty && targetElp.getsPropertyName().nonEmpty) {
									val dtsp = (pm.getdPropertyName(), targetElp.getTargetEntityType, targetElp.getsPropertyId(), targetElp.getsPropertyName())
									targetEntityProps += dtsp
								} else {
									logWarning(s"${pm.getdPropertyName()} has sPropertyId pr sPropertyName is empty ")
								}
							} else {
								logWarning(s"${pm.getdPropertyName()} from entity, but entity is null")
							}

						}
					}
				}

			}
		}
	}

	def unionArrayBuf(tPropsRDD: ArrayBuffer[RDD[(String, ArrayBuffer[(String, String)])]]): RDD[(String, ArrayBuffer[(String, String)])] = {
		var rdd0: RDD[(String, ArrayBuffer[(String, String)])] = tPropsRDD(0)
		rdd0.foreach(f => println(s"rdd0 ===> ${f}"))
		var i = 0
		tPropsRDD.foreach(rdd => {
			if (i != 0) {
				rdd.foreach(f => println(s"rdd${i} ===> ${f}"))
				if (!rdd.isEmpty()) {
					rdd0 = rdd0.union(rdd)
				}
			}
			i += 1
		})
		// val trdd = rdd0.reduceByKey((a, b) => a.union(b))
		// trdd.foreach(f => println(s"trdd ====> ${f}"))
		rdd0
	}

	/**
	  * left outer join
	  *
	  * @param oriRdd
	  * @param otherRdd
	  * @return
	  */
	def joinAndCompactRDD(oriRdd: RDD[(String, ArrayBuffer[(String, String)])], otherRdd: RDD[(String, ArrayBuffer[(String, String)])]): RDD[(String, ArrayBuffer[(String, String)])] = {
		val outerJoinRDD = oriRdd.leftOuterJoin(otherRdd)
		outerJoinRDD.map(m => {
			val tmpBuffer = m._2._1
			val optBuffer: Option[ArrayBuffer[(String, String)]] = m._2._2
			val reBuffer = if (m._2._2.nonEmpty) {
				(m._1, tmpBuffer.union(optBuffer.get))
			} else {
				(m._1, tmpBuffer)
			}
			reBuffer
		})
	}

	/**
	  * get the avrg size of data, and unit is M.
	  *
	  * @param rdd
	  * @return size of one data, unit is M
	  */
	def computeDataSetAvrgSize(rdd: RDD[(String, ArrayBuffer[(String, String)])]): Int = {
		val partitions = rdd.getNumPartitions
		val sampleSize = rdd.mapPartitions(mp => mp.take(1)).map(m => {
			var vTotal = m._1.getBytes.length
			m._2.foreach(f => {
				val v0 = f._1.getBytes().length
				val v1 = f._2.getBytes().length
				vTotal += v0
				vTotal += v1
			})
			(0, vTotal)
		}) //.reduce((x, y) => x + y)
			.reduceByKey((x, y) => x + y).collect()(0)._2
		sampleSize / partitions
	}

	/**
	  * bytes => M
	  *
	  * @param totalCount
	  * @param avrgSize
	  * @return unit is M
	  */
	def getTotalSize(totalCount: Long, avrgSize: Long): Long = {
		(totalCount * avrgSize) / 1024 / 1204
	}

	def getRepartitions(totalSize: Long, blockSize: Long): Long = {
		totalSize / blockSize
	}

	def main(args: Array[String]): Unit = {

		checkArgs(args)

		val docType = args(0)
		if (!DocType.values().contains(DocType.valueOf(docType))) {
			logError(s"docType: ${docType} is not exist.")
			throw new IllegalArgumentException(s"docType: ${docType} is not exist.")
		}

		val sparkConf = new SparkConf()
			.setAppName(s"DocDataCollectJob-${docType}")
		logInfo(s"Starting DocDataCollectJob with spark.app.name: ${sparkConf.get("spark.app.name")}")
		if (sparkConf.getBoolean("spark.execute.local.model", true)) sparkConf.setMaster("local[4]")

		val taskType = sparkConf.get("spark.batch.doc.task.type", "FULL")
		if (!DocTaskType.values().contains(DocTaskType.valueOf(taskType))) {
			logError(s"DocTaskType: ${taskType} is not exist.")
			throw new IllegalArgumentException(s"DocTaskType: ${taskType} is not exist.")
		}

		val elpModelId = sparkConf.get("spark.batch.doc.elp.model", "standard_model")

		var minTimeStamp: Long = 0
		var maxTimeStamp: Long = new Date().getTime

		val sc = new SparkContext(sparkConf)

		val logMap = new util.HashMap[String, Object]()
		try {
			// record the task info
			logMap.put("docType", docType)
			logMap.put("startTime", new Date(sc.startTime))
			logMap.put("sparkMaster", sparkConf.get("spark.master"))
			logMap.put("status", TaskElementStatus.append.toString)
			logMap.put("lastSynTimeStamp", maxTimeStamp.toString)

			val sqlExecutor = new DocMongoExecutor(sc)
			val residSet = sqlExecutor.getAllResIdInFile(sparkConf.get("spark.batch.doc.file.resid.collection", "FileDataResource"))
			val residSetBroadcast = sc.broadcast(residSet)

			val extractorRules = sqlExecutor.getDocType("DocCardExtractorRule", docType).collect()
			if (extractorRules.length != 1) {
				logError("has not only one extractor rule")
				logMap.put("status", TaskElementStatus.interrupted.toString)
				throw new Exception("has not only one extractor rule")
			}
			val extractorRule = JSON.parseObject(extractorRules(0), classOf[DocCardExtractorRule])
			logMap.put("elpModel", extractorRule.getElpModel)
			logMap.put("elpType", extractorRule.getElpType)
			logMap.put("docCollectionName", extractorRule.getDocCollectionName)
			val orderedRule = orderRule(extractorRule)
			if (DocTaskType.INC.toString.equals(taskType)) {
				minTimeStamp = sqlExecutor.getLastTask(taskHistoryCollection, docType, "successful", elpModelId)
			}
			val hBaseExecutor = new HBaseExecutor(sc, residSetBroadcast)
			// 1. from hbase in originalEntity
			val originalEntityRule = orderedRule._2
			// elpModel_entity_elpType
			val originalTableName = orderedRule._1
				.concat(Contants.HBASE_TABLE_SEPERATOR)
				.concat(Contants.HBASE_TABLE_NAME_ENTITY)
				.concat(Contants.HBASE_TABLE_SEPERATOR)
				.concat(originalEntityRule._1)

			val originalKey = originalEntityRule._2
			val originalResultRDD = hBaseExecutor.scan(originalTableName
				// TODO sProperties duplicate
				, hBaseExecutor.buildScan(Contants.FAMILY_PROPERTY_BYTES, originalKey, originalEntityRule._3.map(m => m._1), minTimeStamp, maxTimeStamp))
			val originRDD = hBaseExecutor.computeRDD(originalResultRDD, Contants.FAMILY_PROPERTY_BYTES, originalKey, originalEntityRule._3)
			// data to heavy
			val distinctRDD = originRDD.reduceByKey((x, y) => x)
			val distinctPartitions = distinctRDD.getNumPartitions
			val totalCount = distinctRDD.mapPartitions(mp => mp.map(m => (0, 1))).reduceByKey((x, y) => x + y).collect()(0)._2
			val avrgSize = computeDataSetAvrgSize(distinctRDD)
			val totalSize = getTotalSize(totalCount, avrgSize)
			val blockSizeConf = sparkConf.get("spark.batch.doc.repartition.data.size", "256M")
			val blockSize = blockSizeConf.toUpperCase.substring(0, blockSizeConf.indexOf("M")).toLong
			logInfo(s"totalCount:${totalCount}, avrgSize: ${avrgSize}, totalSize: ${totalSize}, blockSize: ${blockSize}, rddPartitions:${distinctPartitions}")
			// if data.size > 10g or originPartitions > 10, to repartition
			val originPropsRDD = if ((totalSize > 10240 || distinctPartitions > 20) && totalSize > blockSize) {
				val repartitionNum = getRepartitions(totalSize, blockSize).toInt
				logInfo(s"beginning to repartition, repartitionNum is : ${repartitionNum}")
				distinctRDD.repartition(repartitionNum)
			} else {
				logInfo("do without repartition.")
				distinctRDD
			}
			originPropsRDD.persist(StorageLevel.MEMORY_AND_DISK)

			logMap.put("status", TaskElementStatus.running.toString)
			logMap.put("count", totalCount.toString)
			//logInfo(s"docType: ${docType}, 原始数据总量: ${originRDD.count()} ,数据去重后总量: ${total}")
			logInfo(s"docType: ${docType}, totalNum: ${totalCount}")

			// from hbase in extension: targets after rel
			// ArrayBuffer((dPLevel1, extensionType, sPId, sPLevel1), (dPLevel2, extensionType, sPIdLevel2, sPLevel2))
			// (key, (String, String), (String, ArrayBuffer[(String, String)]))
			// ==> (key, ArrayBuffer[(String, String)]
			val extensionRDD = {
				hBaseExecutor.computeExtensionRDDs(orderedRule._1,
					Contants.HBASE_TABLE_NAME_RELATION,
					Contants.FAMILY_PROPERTY_BYTES,
					orderedRule._5,
					minTimeStamp, maxTimeStamp
				)
			}

			val originExtPropsRDD = if (extensionRDD.nonEmpty) {
				val tmpRDD = unionArrayBuf(extensionRDD)
				// originPropsRDD.union(tmpRDD).reduceByKey((a, b) => a.union(b))
				joinAndCompactRDD(originPropsRDD, tmpRDD)
			} else {
				originPropsRDD
			}

			// 2. from hbase in targetEntities
			// ArrayBuffer((dP, targetEntityType, sPId, sP)),
			// ArrayBuffer[RDD[(String, ArrayBuffer[(String, String)])]]
			logInfo("=============================================")
			val tPropsRDD = hBaseExecutor.computeTargetRDDs(orderedRule._1,
				Contants.HBASE_TABLE_NAME_ENTITY,
				Contants.FAMILY_PROPERTY_BYTES,
				orderedRule._3,
				minTimeStamp, maxTimeStamp)

			// 3. in targetRelations
			val relPropsRDD = hBaseExecutor.computeTargetRDDs(orderedRule._1,
				Contants.HBASE_TABLE_NAME_RELATION,
				Contants.FAMILY_PROPERTY_BYTES,
				orderedRule._4,
				minTimeStamp, maxTimeStamp)

			// 4. join
			// 5. write to solr
			val solrExecutor = new SolrExecutor(sc)
			if (tPropsRDD.nonEmpty && relPropsRDD.nonEmpty) {
				val trdd = unionArrayBuf(tPropsRDD)

				val relrdd = unionArrayBuf(relPropsRDD)

				val trddJoinRDD = joinAndCompactRDD(originPropsRDD, trdd)
				val trddAndRelJoinRDD = joinAndCompactRDD(trddJoinRDD, relrdd)

				solrExecutor.save(solrExecutor.buildDoc(docType, trddAndRelJoinRDD))
			} else if (tPropsRDD.nonEmpty && relPropsRDD.isEmpty) {
				val trdd = unionArrayBuf(tPropsRDD)
				val ordd = joinAndCompactRDD(originExtPropsRDD, trdd)
				solrExecutor.save(solrExecutor.buildDoc(docType, ordd))
			} else if (tPropsRDD.isEmpty && relPropsRDD.nonEmpty) {
				val relrdd = unionArrayBuf(relPropsRDD)
				val ordd = joinAndCompactRDD(originExtPropsRDD, relrdd)
				solrExecutor.save(solrExecutor.buildDoc(docType, ordd))
			} else {
				solrExecutor.save(solrExecutor.buildDoc(docType, originExtPropsRDD))
			}

			logMap.put("status", TaskElementStatus.successful.toString)
		} catch {
			case ex: Exception => {
				logMap.put("status", TaskElementStatus.failure.toString)
				logError(s"something worng: ${ex}, ${ex.printStackTrace()}")
			}
		} finally {
			logMap.put("endTime", new Date())
			var options = Map[String, String](
				"host" -> sparkConf.get("spark.batch.mongodb.host", "172.30.6.14"),
				"database" -> sparkConf.get("spark.batch.mongodb.db", "hyjj"),
				"user" -> sparkConf.get("spark.batch.mongodb.db.user", "zqykj"),
				"password" -> sparkConf.get("spark.batch.mongodb.db.password", "zqykj")
			)
			val loadMongoDao = new LoadMongoDao(options)
			loadMongoDao.save(logMap, sparkConf.get("spark.batch.mongodb.db.doc.history", "DocExtractorTaskHistory"))
		}
	}

}
