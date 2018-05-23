package com.zqykj.batch.transform

import com.alibaba.fastjson.JSON
import com.zqykj.batch.transform.common.TransTaskType
import com.zqykj.hyjj.entity.elp.ElpModelDBMapping
import com.zqykj.streaming.common.Contants
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.zqykj.streaming.common.Contants.{ELP_MODELS, ELP_MODEL_DB_MAPPING}
import com.zqykj.streaming.job.SqlExecutor
import com.zqykj.streaming.metadata.ELpModifyMap
import com.zqykj.streaming.service.LoadMongoService

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author alfer
  * @date 1/22/18
  */
object DataTransformJob extends Logging with Serializable {

	val sparkConf = new SparkConf()
	sparkConf.setAppName("DataTransformJob")
		.set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")

	if (sparkConf.getBoolean("spark.execute.local.model", true)) {
		sparkConf.setMaster("local[4]")
	}

	val options = Map[String, String](
		"host" -> sparkConf.get("spark.streaming.mongodb.host", "172.30.6.25"),
		"database" -> sparkConf.get("spark.streaming.mongodb.db", "hyjj"),
		"user" -> sparkConf.get("spark.streaming.mongodb.db.user", "zqykj"),
		"password" -> sparkConf.get("spark.streaming.mongodb.db.password", "zqykj")
	)

	val elpModelId = sparkConf.get("spark.trans.elp.modelId", "standard_model")

	// {"elp": "standard_model","entities": [{"elpTypeId": "phone_number","updateId": "0","dataSchemaId": "ea621045-a28f-49e7-9d37-8af340205372"}]}
	val elpConfig =
		sparkConf.get("spark.trans.entities.with.links", "{\"elp\": \"standard_model\",\"entities\": [{\"dataSchemaId\": \"c8ff84c0-66cc-40b9-b737-84581ed8b31c\",\"elpTypeId\": \"plane\",\"updateId\": 1521213654276}]}")

	private def checkTaskType = {
		// SUB FULL
		val taskType = sparkConf.get("spark.trans.task.type", "SPE")
		try {
			TransTaskType.valueOf(taskType)
			logInfo(s"DataTransformJob start in ${taskType.toString} task mode")
		} catch {
			case ex: IllegalArgumentException => {
				val errorMsg = s"batch to transform data does not support the taskType: ${taskType}, only support ${TransTaskType.values}"
				logError(errorMsg)
				throw new IllegalArgumentException(errorMsg)
			}
		}
		taskType
	}

	def getAllDataSchemaIds(sc: SparkContext, mode: String): mutable.HashSet[String] = {
		val hadoopConfiguration = sc.hadoopConfiguration
		if (sparkConf.getBoolean("spark.execute.local.model", true))
			hadoopConfiguration.set(FileSystem.FS_DEFAULT_NAME_KEY, sparkConf.get("spark.trans.fs.defaultFS", "hdfs://test82:8020"))
		val fileSystem = FileSystem.get(hadoopConfiguration)
		val path = new Path(sparkConf.get("spark.trans.data.path", "/user/wf/mvp/data"))
		val dsIdList = fileSystem.listStatus(path)
		val set = new mutable.HashSet[String]()

		for (dsId <- dsIdList) {
			if (dsId.isDirectory) {
				val dsid = dsId.getPath.getName
				logInfo(s"=========> dsId: ${dsid}")
				set.add(dsid)
			}
		}
		set
	}

	def getElpModifyByParseJsonConfig(loadMongoService: LoadMongoService): ELpModifyMap = {
		val elpMap = if (loadMongoService.isExistByElp(elpModelId, Contants.ELP_TRANS_ENTITIS_LINKS)) {
			loadMongoService.getELpMapByElpId(elpModelId, Contants.ELP_TRANS_ENTITIS_LINKS)
		} else {
			val elpModifyMap: ELpModifyMap = if (Option(elpConfig).isEmpty
				|| "{}".equals(elpConfig)) {
				logInfo(s"接受的实体链接为空, elpConfig: ${elpConfig}")
				// throw new Exception(s"elpMofifyObj 为空, elpConfig: ${elpConfig}")
				return null
			} else {
				var elpMofifyObj: ELpModifyMap = null
				try {
					elpMofifyObj = JSON.parseObject(elpConfig, classOf[ELpModifyMap])
					if (Option(elpMofifyObj).isEmpty) {
						throw new RuntimeException(s"elpMofifyObj 为空, elpConfig: ${elpConfig}")
					}
				} catch {
					case ex: Exception => throw new Exception(s"JSON 解析异常, elpConfig: ${elpConfig}", ex)
				}
				elpMofifyObj
			}
			elpModifyMap
		}
		elpMap
	}

	def main(args: Array[String]): Unit = {

		val taskType = checkTaskType
		sparkConf.getAll.foreach(f => logInfo(s"key: ${f._1}, value: ${f._2}"))

		// -1: read add all
		val sequenceId = sparkConf.get("spark.trans.sequenceId", "89224352871229444")
		val sc = new SparkContext(sparkConf)
		val sqlExecutor = new SqlExecutor(sc)
		val cacheElpDBMappings = sqlExecutor.cacheElpDBMappings(sqlExecutor.loadData(ELP_MODEL_DB_MAPPING), elpModelId)

		val loadMongoService = new LoadMongoService(options)
		val specialDataSchemaId = sparkConf.get("spark.trans.special.dataschemaId", "e4e09730-ff8a-4b56-b2c3-c900833785ed")

		// taskType: full, data from all dataSchemaIds
		val dataSchemaIdAndMappings: (mutable.HashSet[String], mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]) =
			if (TransTaskType.FULL.toString.equals(taskType)) {
				val elpMap = getElpModifyByParseJsonConfig(loadMongoService)
				if (Option(elpMap).nonEmpty) {
					loadMongoService.upInsert(elpMap, Contants.ELP_TRANS_ENTITIS_LINKS)
				}
				val dataSchemaIds = getAllDataSchemaIds(sc, "FULL")
				(dataSchemaIds, cacheElpDBMappings)
			} else if (TransTaskType.SUB.toString.equals(taskType)) {
				val elpMap = getElpModifyByParseJsonConfig(loadMongoService)
				val dataSchemaIds = loadMongoService.getDataSchemaIdsByElpId(elpModelId, elpMap)
				val mappings = loadMongoService.getElpDBMappings(dataSchemaIds, elpMap, cacheElpDBMappings)
				(dataSchemaIds, mappings)
			} else if (TransTaskType.SPE.toString.equals(taskType)) {
				val dataSchemaIds = new mutable.HashSet[String]()
				val dsidAndDBMapping = new mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]
				dataSchemaIds.+=(specialDataSchemaId)
				val mappings = cacheElpDBMappings.get(specialDataSchemaId).get
				dsidAndDBMapping.put(specialDataSchemaId, mappings)
				(dataSchemaIds, dsidAndDBMapping)
			} else {
				val errorMsg = s"batch to transform data does not support the taskType: ${taskType}, only support ${TransTaskType.values}"
				logError(errorMsg)
				throw new IllegalArgumentException(errorMsg)
			}

		for (s <- dataSchemaIdAndMappings._1) {
			println(s"==dataSchemaId==>: ${s}")
		}

		val elpDBMappingByELPBroad = sc.broadcast(dataSchemaIdAndMappings._2)
		val elpModelCache = sqlExecutor.cacheElpModel(sqlExecutor.loadData(ELP_MODELS), elpModelId)
		val elpModelCacheBroadcast = sc.broadcast(elpModelCache)
		val elpModelAndMappingsBroadcast = sc.broadcast(sqlExecutor.cacheElpEntitiesAndLinks(elpModelCache))

		val transformExecutor = new BulkTransformExecutor(sc, dataSchemaIdAndMappings._1, sequenceId.toLong,
			elpModelCacheBroadcast, elpDBMappingByELPBroad, elpModelAndMappingsBroadcast)
		transformExecutor.execute()

	}

}
