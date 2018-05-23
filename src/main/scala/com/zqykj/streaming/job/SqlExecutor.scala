package com.zqykj.streaming.job

import com.alibaba.fastjson.JSON
import com.google.common.collect.ImmutableList
import com.zqykj.hyjj.entity.elp._
import com.zqykj.streaming.common.{Contants, JobPropertyConstant}
import com.zqykj.streaming.common.Contants.{PropertyTypeConstants, RMDB}
import com.zqykj.streaming.datepattern.DatePatternConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.zqykj.streaming.metadata.{ELpModifyMap, ELpTypeModifyMap, ElpTopicMapping, OriginDataSchema}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by alfer on 8/31/17.
  */
class SqlExecutor(val sc: SparkContext) {

	val sparkConf = sc.getConf

	def loadData(collection: String): DataFrame = {
		val mongoContext: SQLContext = new SQLContext(sc)
		val options = Map[String, String](
			"host" -> sparkConf.get("spark.streaming.mongodb.host", JobPropertyConstant.MONGODB_HOST_DEFAULT),
			"database" -> sparkConf.get("spark.streaming.mongodb.db", "hyjj"),
			"collection" -> collection
			, "credentials" -> "zqykj,hyjj,zqykj"
		)
		println(s"options: ${options}")
		val df = mongoContext.read.format("com.stratio.datasource.mongodb").options(options).load()
		df
	}

	def loadDataFromRMDB(driver: String, url: String, dbtable: String): DataFrame = {
		val rmdbSqlContext: SQLContext = new SQLContext(sc)
		val options = Map[String, String](
			"driver" -> driver,
			"url" -> url,
			"user" -> sparkConf.get("spark.streaming.mysql.username", "root"),
			"password" -> sparkConf.get("spark.streaming.mysql.password", "123456"),
			"dbtable" -> dbtable
		)
		rmdbSqlContext.read.format("jdbc").options(options).load()
	}

	def getBlacklistFromMySQL(): java.util.ArrayList[String] = {
		val sql = "(select phone_number FROM isp_numbers WHERE is_use = 1) as phnums"
		val df = loadDataFromRMDB(RMDB.MYSQL_DRIVER_NAME,
			sparkConf.get("spark.streaming.mysql.url", "jdbc:mysql://172.30.6.20:3306/simulate_city"), sql)
		val list = new java.util.ArrayList[String]()
		df.collect().foreach(s => {
			list.add(s.getString(0))
		})
		//        import scala.collection.JavaConversions._
		//        for (l <- list) {
		//            println(l)
		//        }
		//        println(s"list.size=${list.size()}")
		list
	}

	def cacheOriDataSchema(df: DataFrame): mutable.HashMap[String, OriginDataSchema] = {
		val originDataSchemaMap = new mutable.HashMap[String, OriginDataSchema]
		df.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[OriginDataSchema])
			originDataSchemaMap.put(obj.getTypeId, obj)
		})
		originDataSchemaMap
	}

	def cacheElpModels(df: DataFrame): mutable.HashMap[String, ElpModel] = {
		val elpModelMap = new mutable.HashMap[String, ElpModel]
		df.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[ElpModel])
			elpModelMap.put(obj.getModelId, obj)
		})
		elpModelMap
	}

	def cacheElpModel(df: DataFrame, elpModelId: String): mutable.HashMap[String, ElpModel] = {
		val elpModelMap = new mutable.HashMap[String, ElpModel]
		val elpDF = df.filter(df.col("modelId").equalTo(elpModelId))
		val rows = elpDF.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[ElpModel])
			if (obj.getModelId.equals(elpModelId)) {
				elpModelMap.put(obj.getModelId, obj)
			}
		})
		elpModelMap
	}


	def cacheElpEntitiesAndLinks(cacheElps: mutable.HashMap[String, ElpModel]): mutable.HashMap[String, PropertyBag] = {
		val bagMap = new mutable.HashMap[String, PropertyBag]()
		cacheElps.map(m => {
			val entitiesIter: java.util.Iterator[Entity] = m._2.getEntities.iterator()
			while (entitiesIter.hasNext) {
				val entity = entitiesIter.next()
				bagMap.put(m._1 + Contants.ELP_MAPPING_SEPARATOR
					+ entity.getUuid + Contants.ELP_MAPPING_SEPARATOR
					+ Contants.ENTITY,
					entity)
			}

			val relationIter: java.util.Iterator[Link] = m._2.getLinks.iterator()
			while (relationIter.hasNext) {
				val link = relationIter.next()
				bagMap.put(m._1 + Contants.ELP_MAPPING_SEPARATOR
					+ link.getUuid + Contants.ELP_MAPPING_SEPARATOR
					+ Contants.LINK,
					link)

			}
		}
		)
		bagMap
	}

	def cacheElpDBMappings(df: DataFrame): mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]] = {
		val elpDBMapping = new mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]
		df.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[ElpModelDBMapping])
			val dataSchemaId = obj.getDataSchemaId
			// val dsTable = obj.getDs + Contants.DSID_DS_TABLENAME + obj.getTableName

			if (elpDBMapping.contains(dataSchemaId)) {
				elpDBMapping.get(dataSchemaId).get.+=(obj)
			} else {
				val mappingBuffer = new ArrayBuffer[ElpModelDBMapping]()
				mappingBuffer.+=(obj)
				elpDBMapping.put(dataSchemaId, mappingBuffer)
			}

		})
		elpDBMapping
	}

	def cacheElpDBMappings(df: DataFrame, elpModelId: String): mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]] = {
		val elpDBMapping = new mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]
		df.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[ElpModelDBMapping])
			val dataSchemaId = obj.getDataSchemaId
			// val dsTable = obj.getDs + Contants.DSID_DS_TABLENAME + obj.getTableName
			if (obj.getElp.equals(elpModelId)) {
				if (elpDBMapping.contains(dataSchemaId)) {
					elpDBMapping.get(dataSchemaId).get.+=(obj)
				} else {
					val mappingBuffer = new ArrayBuffer[ElpModelDBMapping]()
					mappingBuffer.+=(obj)
					elpDBMapping.put(dataSchemaId, mappingBuffer)
				}
			}
		})
		elpDBMapping
	}

	/**
	  *
	  * @param df
	  * @param elpModelId
	  * @return
	  */
	def cacheELpModifyTransMap(df: DataFrame, elpModelId: String): mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]] = {
		val eLpModifyMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.HashSet[String]]]()
		df.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[ELpModifyMap])
			if (obj.getElp.equals(elpModelId)) {
				val entities = new mutable.HashMap[String, mutable.HashSet[String]]()
				if (Option(obj.getEntities).nonEmpty) {
					for (entity: ELpTypeModifyMap <- obj.getEntities.asScala) {
						if (!entities.contains(entity.getDataSchemaId)) {
							val elpTypeIds = new mutable.HashSet[String]()
							elpTypeIds.add(entity.getElpTypeId)
							entities.put(entity.getDataSchemaId, elpTypeIds)
						} else {
							entities.get(entity.getDataSchemaId).get.add(entity.getElpTypeId)
						}
					}
					eLpModifyMap.put(Contants.ENTITY, entities)
				}

				val links = new mutable.HashMap[String, mutable.HashSet[String]]()
				if (Option(obj.getLinks).nonEmpty) {
					for (link: ELpTypeModifyMap <- obj.getLinks.asScala) {
						if (!links.contains(link.getDataSchemaId)) {
							val elpTypeIds = new mutable.HashSet[String]()
							elpTypeIds.add(link.getElpTypeId)
							links.put(link.getDataSchemaId, elpTypeIds)
						} else {
							links.get(link.getDataSchemaId).get.add(link.getElpTypeId)
						}
					}
					eLpModifyMap.put(Contants.LINK, links)
				}
			}
		})
		eLpModifyMap
	}

	def cacheElpTopicMappings(df: DataFrame): mutable.HashMap[String, ElpTopicMapping] = {
		val elpTopicMapping = new mutable.HashMap[String, ElpTopicMapping]()
		df.toJSON.collect().foreach(s => {
			val obj = JSON.parseObject(s, classOf[ElpTopicMapping])
			elpTopicMapping.put(obj.getTopic, obj)
		})
		elpTopicMapping
	}

	def cacheSystemPatternDateConfig(df: DataFrame): mutable.HashMap[String, java.util.List[String]] = {
		val datePatternMap = new mutable.HashMap[String, java.util.List[String]]
		val systemPatternDateConfig = df.filter(df.col(DatePatternConfig.CODE).equalTo(DatePatternConfig.DATE_PATTERN_CODE)).toJSON.collect()
		if (Option(systemPatternDateConfig).nonEmpty && systemPatternDateConfig.size > 0) {
			val config = systemPatternDateConfig(0)
			val datePatternConfigObj = JSON.parseObject(config, classOf[DatePatternConfig])
			val params = datePatternConfigObj.getValue
			val dateConfig = params.getDate()
			val datetimeConfig = params.getDatetime()
			val timeConfig = params.getTime()
			if (StringUtils.isNotBlank(dateConfig)) {
				val datePattern: java.util.List[String] = ImmutableList.copyOf(dateConfig.split(DatePatternConfig.SPACK_MARK))
				datePatternMap.put(PropertyTypeConstants.date, datePattern)
			}

			if (StringUtils.isNotBlank(datetimeConfig)) {
				val datetimePattern: java.util.List[String] = ImmutableList.copyOf(datetimeConfig.split(DatePatternConfig.SPACK_MARK))
				datePatternMap.put(PropertyTypeConstants.datetime, datetimePattern)
			}

			if (StringUtils.isNotBlank(timeConfig)) {
				val timePattern: java.util.List[String] = ImmutableList.copyOf(timeConfig.split(DatePatternConfig.SPACK_MARK))
				datePatternMap.put(PropertyTypeConstants.time, timePattern)
			}
		}
		datePatternMap
	}

}
