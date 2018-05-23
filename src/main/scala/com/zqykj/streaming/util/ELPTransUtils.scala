package com.zqykj.streaming.util

import java.util
import java.util.{Date, Set}
import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zqykj.hyjj.entity.elp.{ElpModelDBMapping, PropertyBag, PropertyType}
import com.zqykj.streaming.common.{Contants, JobConstants}
import com.zqykj.streaming.common.Contants._
import com.zqykj.streaming.metadata.{ELpModifyMap, ELpTypeModifyMap}
import com.zqykj.streaming.service.LoadMongoService
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Created by alfer on 9/9/17.
  */
object ELPTransUtils extends Logging with Serializable {

	val templateVarMap = new util.HashMap[String, Set[String]]

	def parseVarNames(labelTemplate: String): util.Set[String] =
		if (labelTemplate != null && labelTemplate.length != 0) {
			if (!templateVarMap.containsKey(labelTemplate)) {
				val p = Pattern.compile("\\$\\{(.+?)\\}")
				val m = p.matcher(labelTemplate)
				val ret = new util.HashSet[String]
				while (m.find) {
					ret.add(m.group(1))
				}
				templateVarMap.put(labelTemplate, ret)
			}
			val varNames: Set[String] = templateVarMap.get(labelTemplate)
			// .asInstanceOf[util.Set[String]]
			varNames
		}
		else {
			new util.HashSet[String]
		}

	def findColName(fieldUuid: String, dbMap: ElpModelDBMapping): String = {
		val dbMapPropColPairList = dbMap.getDbmap
		var colName: String = null
		val dbMapIterator = dbMapPropColPairList.iterator()
		var foundIt = false
		while (dbMapIterator.hasNext && !foundIt) {
			val propColPair = dbMapIterator.next()
			if (propColPair.getPropertyUuid.equals(fieldUuid)) {
				colName = propColPair.getColumnName
				foundIt = true
			}
		}
		colName
	}

	def parseCols(id: StringBuilder, cols: Option[java.util.List[String]], jsonBody: JSONObject): StringBuilder = {
		if (cols.nonEmpty && cols.get.size() > 0) {
			for (sCol <- cols.get.asScala) {
				val colValue = Option(jsonBody.getString(sCol))
				if (colValue.nonEmpty && colValue.get.trim.size > 0) {
					if (id.length > 0) id.append(Contants.ID_SPACE_MARK)
					id.append(colValue.get)
				} else {
					return null
				}
			}
			id
		} else {
			return null
		}
	}

	def getCollectionNameByTableName(elpIdAndType: String, elpType: String): String = {
		val collectionName = if (Contants.ENTITY.equals(elpType)) {
			elpIdAndType.substring(0, elpIdAndType.indexOf(EntityConstants.SOLR_COLLECTION_SEPERATOR))
				.concat(EntityConstants.SOLR_COLLECTION_SUFFIX)
		} else if (Contants.LINK.equals(elpType)) {
			elpIdAndType.substring(0, elpIdAndType.indexOf(LinkContants.SOLR_COLLECTION_SEPERATOR))
				.concat(LinkContants.SOLR_COLLECTION_SUFFIX)
		} else {
			null
		}
		collectionName
	}

	/**
	  *
	  * @param elpModifyMap : revice
	  * @param map          : lastLoadJob elp at lastTime
	  * @return (received ELPModifyMap, need filtered entities or links)
	  */
	def filterModifiedEL(elpModifyMap: ELpModifyMap, map: mutable.HashMap[String, String]): (ELpModifyMap, mutable.HashSet[String]) = {
		val filteredMap = new mutable.HashSet[String]()
		val updateElpMap = new ELpModifyMap
		updateElpMap.setElp(elpModifyMap.getElp)
		val updateEntities = new util.ArrayList[ELpTypeModifyMap]()
		val updateLinks = new util.ArrayList[ELpTypeModifyMap]()
		val elpId = elpModifyMap.getElp
		if (Option(elpModifyMap.getEntities).nonEmpty) {
			elpModifyMap.getEntities.asScala.filter(ele => {
				Option(ele.getElpTypeId).nonEmpty && Option(ele.getDataSchemaId).nonEmpty && Option(ele.getUpdateId).nonEmpty
			}).filter(entity => {
				// elpId_entity_elpTypeId
				val value = map.get(elpId.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(EntityConstants.ELP_ENTITY)
					.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(entity.getElpTypeId))
				if (value.nonEmpty) {
					val i = entity.getUpdateId.compare(value.get)
					if (i > 0) true else false
				} else {
					false
				}
			}).foreach(entity => {
				filteredMap.add(elpId.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(EntityConstants.ELP_ENTITY)
					.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(entity.getElpTypeId))
				updateEntities.add(entity)
			})
		}

		if (Option(elpModifyMap.getLinks).nonEmpty) {
			elpModifyMap.getLinks.asScala.filter(ele => {
				Option(ele.getElpTypeId).nonEmpty && Option(ele.getDataSchemaId).nonEmpty && Option(ele.getUpdateId).nonEmpty
			}).filter(link => {
				// elpId_link_elpTypeId
				val value = map.get(elpId.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(LinkContants.ELP_LINK)
					.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(link.getElpTypeId))
				if (value.nonEmpty) {
					val i = link.getUpdateId.compare(value.get)
					if (i > 0) true else false
				} else {
					false
				}
			}).foreach(link => {
				filteredMap.add(elpId.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(LinkContants.ELP_LINK)
					.concat(Contants.ELP_MAPPING_SEPARATOR)
					.concat(link.getElpTypeId))
				updateLinks.add(link)
			})
		}
		updateElpMap.setEntities(updateEntities)
		updateElpMap.setLinks(updateLinks)

		(updateElpMap, filteredMap)
	}


	def getfilteredELP(sparkConf: SparkConf, options: Map[String, String]): mutable.HashSet[String] = {
		val zkQum = sparkConf.get("spark.kafka.stream.transform.zkqurom", "test82:2181")

		DistIdUtils.apply(zkQum)

		val sequenceId = DistIdUtils.getSequenceId()
		logInfo(s"load.sequence.id: ${sequenceId}")
		sparkConf.set("load.sequence.id", "" + sequenceId)
		DistIdUtils.close()

		// {"elp" : "standard_model"}
		val elpEntitiesAndLinksConfig = sparkConf.get("spark.kafka.stream.transform.entities.with.links", "{}")
		val filteredELP: mutable.HashSet[String] = if (Option(elpEntitiesAndLinksConfig).isEmpty
			|| "{}".equals(elpEntitiesAndLinksConfig)) {
			logInfo(s"接受的实体链接为空, elpEntitiesAndLinksConfig: ${elpEntitiesAndLinksConfig}")
			new mutable.HashSet[String]()
		} else {
			logInfo(s"接受的实体链接为: elpEntitiesAndLinksConfig: ${elpEntitiesAndLinksConfig}")
			val loadMongoService = new LoadMongoService(options)
			var elpMofifyObj: ELpModifyMap = null
			try {
				elpMofifyObj = JSON.parseObject(elpEntitiesAndLinksConfig, classOf[ELpModifyMap])
				if (Option(elpMofifyObj).isEmpty) {
					throw new RuntimeException(s"elpMofifyObj 为空, elpEntitiesAndLinksConfig: ${elpEntitiesAndLinksConfig}")
				}
			} catch {
				case ex: Exception => throw new Exception(s"JSON 解析异常, elpEntitiesAndLinksConfig: ${elpEntitiesAndLinksConfig}", ex)
			}
			elpMofifyObj.setSeqId(sequenceId)

			try {
				elpMofifyObj.getEntities.asScala.foreach(ele => {
					if (Option(ele.getElpTypeId).isEmpty) {
						throw new Exception(s"Params of elpEntitiesAndLinksConfig contain empty elpTypeId in Entities, entity: ${ele}")
					}

				})
				elpMofifyObj.getLinks.asScala.foreach(ele => {
					if (Option(ele.getElpTypeId).isEmpty) {
						throw new Exception(s"Params of elpEntitiesAndLinksConfig contain empty elpTypeId in Links, link: ${ele}")
					}
				})
			} catch {
				case ex: Exception => {
					logError(ex.getStackTraceString, ex)
					throw new Exception(ex.getStackTraceString, ex)
				}
			}

			val elpId = elpMofifyObj.getElp

			// lastMap: null
			val isExistStreamCursor = loadMongoService.isExistByElp(elpId, Contants.ELP_STREAM_LOAD_ENTITIES_LINKS)
			if (isExistStreamCursor) {
				val lastMap = loadMongoService.getEntitiesAndLinksByElpId(elpId, Contants.ELP_STREAM_LOAD_ENTITIES_LINKS)
				val elpTypeIdSet: mutable.HashSet[String] = if (lastMap.size == 0) {
					// 持久化的elp没有实体/链接
					loadMongoService.updateElpMap(elpMofifyObj, Contants.ELP_STREAM_LOAD_ENTITIES_LINKS)
					loadMongoService.upInsert(elpMofifyObj, Contants.ELP_TRANS_ENTITIS_LINKS)
					new mutable.HashSet[String]()
				} else {
					// elp有变化才比对
					val needFilterELp = ELPTransUtils.filterModifiedEL(elpMofifyObj, lastMap)
					loadMongoService.updateElpMap(elpMofifyObj, Contants.ELP_STREAM_LOAD_ENTITIES_LINKS)
					// 更新离线任务的游标
					loadMongoService.upInsert(needFilterELp._1, Contants.ELP_TRANS_ENTITIS_LINKS)
					needFilterELp._2
				}
				elpTypeIdSet
			} else {
				// 不存在则创建
				loadMongoService.saveElpMap(elpMofifyObj, Contants.ELP_STREAM_LOAD_ENTITIES_LINKS)
				// 更新离线任务的游标
				loadMongoService.upInsert(elpMofifyObj, Contants.ELP_TRANS_ENTITIS_LINKS)
				new mutable.HashSet[String]()
			}
		}
		filteredELP
	}


}
