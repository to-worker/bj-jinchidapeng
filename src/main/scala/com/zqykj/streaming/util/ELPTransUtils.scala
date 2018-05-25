package com.zqykj.streaming.util

import java.util
import java.util.{Date, Set}
import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zqykj.hyjj.entity.elp._
import com.zqykj.hyjj.query.{CompactLinkData, PropertyData}
import com.zqykj.streaming.common.{Contants, JobBusConstant, JobConstants}
import com.zqykj.streaming.common.Contants._
import com.zqykj.streaming.metadata.{ELpModifyMap, ELpTypeModifyMap}
import com.zqykj.streaming.service.LoadMongoService
import org.apache.commons.lang3.StringUtils
import org.apache.solr.common.SolrInputDocument
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

	/**
	  * transform json format's data to entity data
	  *
	  * @param jsonObj
	  * @param elementEntity
	  * @param dbMap
	  * @return
	  */
	def parseEntity(jsonObj: JSONObject, elementEntity: Entity, dbMap: ElpModelDBMapping): JSONObject = {
		val elpJsonObj = new JSONObject()
		try {
			val jsonBody = jsonObj
			val props = elementEntity.getProperties
			val idCols = Option(dbMap.getIdToColumns)
			// 1: idCols,  2 : idCol 如果标识属性值为空, 则过滤此条记录

			val idStr = {
				val idBuilder = new StringBuilder(elementEntity.getRootSemanticType)
				ELPTransUtils.parseCols(idBuilder, idCols, jsonObj)
			}

			if (Option(idStr).isEmpty) {
				return null
			}

			val resId = jsonObj.getString(EntityConstants.VERTEXT_RESID_UPPER)
			val owner = Option(jsonObj.getString(EntityConstants.VERTEXT_OWNER)).getOrElse("")
			elpJsonObj.put(EntityConstants.HBASE_TABLE_ROWKEY, resId + ID_ELP_TYPE_SEPERATOR + idStr.toString())
			elpJsonObj.put(EntityConstants.VERTEX_ID_FILED, idStr.toString)
			elpJsonObj.put(EntityConstants.VERTEX_TYPE_FILED, elementEntity.getUuid)
			elpJsonObj.put(EntityConstants.VERTEXT_OWNER, owner)
			elpJsonObj.put(EntityConstants.VERTEXT_DSID, resId)

			elpJsonObj.put("body", getJsonBody(props, jsonBody, elementEntity, dbMap))
			logDebug(s"Entity elpJsonObj=${elpJsonObj}")

		} catch {
			case ex: RuntimeException => logError("=============> 解析实体数据异常", ex)
			case ex: Exception => logError("=============> 解析实体数据异常", ex)
				return null
		}
		elpJsonObj
	}

	def parseEntityWithResId(resourceId: String, jsonObj: JSONObject, elementEntity: Entity, dbMap: ElpModelDBMapping): JSONObject = {
		val elpJsonObj = new JSONObject()
		try {
			val jsonBody = jsonObj
			val props = elementEntity.getProperties
			val idCols = Option(dbMap.getIdToColumns)
			// 1: idCols,  2 : idCol 如果标识属性值为空, 则过滤此条记录

			val idStr = {
				val idBuilder = new StringBuilder(elementEntity.getRootSemanticType)
				ELPTransUtils.parseCols(idBuilder, idCols, jsonObj)
			}

			if (Option(idStr).isEmpty) {
				return null
			}

			val resId = resourceId
			elpJsonObj.put(EntityConstants.HBASE_TABLE_ROWKEY, resId + ID_ELP_TYPE_SEPERATOR + idStr.toString())
			elpJsonObj.put(EntityConstants.VERTEX_ID_FILED, idStr.toString)
			elpJsonObj.put(EntityConstants.VERTEX_TYPE_FILED, elementEntity.getUuid)
			elpJsonObj.put(EntityConstants.VERTEXT_DSID, resId)

			elpJsonObj.put("body", getJsonBody(props, jsonBody, elementEntity, dbMap))
			logDebug(s"Entity elpJsonObj=${elpJsonObj}")

		} catch {
			case ex: RuntimeException => logError("=============> 解析实体数据异常", ex)
			case ex: Exception => logError("=============> 解析实体数据异常", ex)
				return null
		}
		elpJsonObj
	}


	def parseEntityWithResIdAtRelation(resourceId: String, jsonBody: JSONObject, elementEntity: Entity, dbMap: ElpModelDBMapping): SolrInputDocument = {
		val solrDocument = new SolrInputDocument()
		try {
			val idStr = {
				val idBuilder = new StringBuilder(elementEntity.getRootSemanticType)
				idBuilder.append(Contants.ID_SPACE_MARK).append(jsonBody.getString(JobBusConstant.RELATION_ID_NAME))
			}

			if (Option(idStr).isEmpty) {
				return null
			}

			val entityId = idStr.toString()
			val idLabel = resourceId + ID_ELP_TYPE_SEPERATOR + entityId
			solrDocument.setField(EntityConstants.HBASE_TABLE_ID, idLabel)
			solrDocument.setField(EntityConstants.VERTEX_ID_FILED, entityId)
			solrDocument.setField(EntityConstants.VERTEX_TYPE_FILED, elementEntity.getUuid)
			solrDocument.setField(EntityConstants.VERTEXT_RESID, resourceId)
			solrDocument.setField(JobBusConstant.RELATION_ID_TYPE_NAME, jsonBody.getString(JobBusConstant.RELATION_ID_TYPE_NAME))
			solrDocument.addField("_indexed_at_tdt", new Date())
		} catch {
			case ex: RuntimeException => logError("=============> 解析实体数据异常", ex)
			case ex: Exception => logError("=============> 解析实体数据异常", ex)
				return null
		}
		solrDocument
	}


	/**
	  * transform json format's data to link data
	  *
	  * @param jsonBody
	  * @param elementLink
	  * @param dbMap
	  * @param elp
	  * @return
	  */
	def parseLink(jsonBody: JSONObject, elementLink: Link, dbMap: ElpModelDBMapping, elp: ElpModel): JSONObject = {
		val elpJsonObj = new JSONObject()
		try {

			if (Option(elementLink.getSourceEntity).isEmpty
				|| Option(elementLink.getTargetEntity).isEmpty) {
				return null
			}

			val props = elementLink.getProperties
			val linkData = new CompactLinkData(elementLink.getUuid)

			for (prop <- props.asScala) {
				val fieldUuid = prop.getUuid
				val colName = ELPTransUtils.findColName(fieldUuid, dbMap)
				if ("" != colName) {
					val value = jsonBody.getString(colName)
					linkData.addProperty(new PropertyData(prop.getName, value))
				}
			}

			val idCols = Option(dbMap.getIdToColumns)
			val idStr = {
				val idBuilder = new StringBuilder(elementLink.getUuid)
				ELPTransUtils.parseCols(idBuilder, idCols, jsonBody)
			}
			if (Option(idStr).isEmpty) {
				return null
			}

			val resId = jsonBody.getString(LinkContants.EDGE_RESID_UPPER)
			val owner = Option(jsonBody.getString(LinkContants.EDGE_OWNER)).getOrElse("")
			elpJsonObj.put(LinkContants.HBASE_TABLE_ROWKEY, resId + ID_ELP_TYPE_SEPERATOR + idStr.toString())

			// 判断并调整链接方向
			var dataDirectivity: Directivity = null
			var needReverseDirection = false
			// dirtected => true: 有向, false: 无向
			if (elementLink.isDirected) {
				if (dbMap.getDirectivity == Directivity.NotDirected) {
					dataDirectivity = Directivity.SourceToTarget
				} else if (Directivity.TargetToSource == dbMap.getDirectivity) {
					needReverseDirection = true
				} else if (Directivity.SourceToTarget == dbMap.getDirectivity) {
					needReverseDirection = false
				} else { // 单一列
					val directionColumn = Option(dbMap.getDirectionColumn)
					if (directionColumn.nonEmpty) {
						dataDirectivity = directionColumn.get.testLinkData(linkData,
							elementLink.getProperty(directionColumn.get.getColumnName))
						dataDirectivity = dataDirectivity match {
							case Directivity.TargetToSource =>
								needReverseDirection = true
								Directivity.SourceToTarget
							case _ =>
								dataDirectivity
						}
					}
				}

				if (Option(dataDirectivity).isEmpty) {
					dataDirectivity = if (dbMap.getDirectivity == Directivity.TargetToSource) Directivity.TargetToSource else dbMap.getDirectivity
				}

				if (!needReverseDirection) { // 不需要调整
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "source", jsonBody, dbMap))
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "target", jsonBody, dbMap))
				} else { // 调整方向
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "source", jsonBody, dbMap))
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "target", jsonBody, dbMap))
				}
			} else {
				dataDirectivity = Directivity.NotDirected
				elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
				elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
					parseEntityId(elementLink, "source", jsonBody, dbMap))
				elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
				elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
					parseEntityId(elementLink, "target", jsonBody, dbMap))
			}

			// 数据方向
			val directionType = dataDirectivity match {
				case Directivity.SourceToTarget =>
					LinkContants.DIRECTION_UNIDIRECTIONAL
				case Directivity.TargetToSource =>
					LinkContants.DIRECTION_UNIDIRECTIONAL
				case Directivity.NotDirected =>
					LinkContants.DIRECTION_UNDIRECTED
				case Directivity.Bidirectional =>
					LinkContants.DIRECTION_BIDIRECTIONAL
				case _ =>
					LinkContants.DIRECTION_UNIDIRECTIONAL
			}
			elpJsonObj.put(LinkContants.EDGE_DIRECTION_TYPE_FIELD, directionType)
			elpJsonObj.put(LinkContants.EDGE_TYPE_FIELD, elementLink.getUuid)
			elpJsonObj.put(LinkContants.EDGE_ID_FIELD, idStr.toString())
			elpJsonObj.put(LinkContants.EDGE_DSID, resId)
			elpJsonObj.put(LinkContants.EDGE_OWNER, owner)
			elpJsonObj.put("body", getJsonBody(props, jsonBody, elementLink, dbMap))
			logDebug(s"Link elpJsonObj=${elpJsonObj}")
		} catch {
			case ex: RuntimeException => logError("=============> 解析链接数据异常", ex)
			case ex: Exception => logError("=============> 解析链接数据异常", ex)
				return null
		}
		elpJsonObj
	}

	def parseLinkWithResIdAtRelation(resourceId: String, sourceJsonObj: JSONObject, targetJsonObj: JSONObject, elementLink: Link,
	                                 dbMap: ElpModelDBMapping): SolrInputDocument = {
		val id_0 = sourceJsonObj.getString(JobBusConstant.RELATION_ID_NAME)
		val id_1 = targetJsonObj.getString(JobBusConstant.RELATION_ID_NAME)
		val id_type_0 = sourceJsonObj.getString(JobBusConstant.RELATION_ID_TYPE_NAME)
		val id_type_1 = targetJsonObj.getString(JobBusConstant.RELATION_ID_TYPE_NAME)

		if (StringUtils.isNotBlank(id_0) || StringUtils.isNotBlank(id_1)) {
			return null
		}

		val compareResult = id_0.compare(id_1)
		var id_label = ""
		var from_entity_id = ""
		var from_entity_type = ""
		var to_entity_id = ""
		var to_entity_type = ""

		val solrDocument = new SolrInputDocument()
		if (compareResult >= 0) {
			id_label = resourceId.concat(ID_ELP_TYPE_SEPERATOR)
				.concat(id_0).concat(Contants.ID_SPACE_MARK)
				.concat(id_1)
			from_entity_type = elementLink.getSourceEntity
			to_entity_type = elementLink.getTargetEntity
			from_entity_id = elementLink.getSourceEntity.concat(Contants.ID_SPACE_MARK).concat(id_0)
			to_entity_id = elementLink.getTargetEntity.concat(Contants.ID_SPACE_MARK).concat(id_1)
			solrDocument.setField(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, from_entity_type)
			solrDocument.setField(LinkContants.EDGE_FROM_VERTEX_ID_FIELD, from_entity_id)
			solrDocument.setField(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, to_entity_type)
			solrDocument.setField(LinkContants.EDGE_TO_VERTEX_ID_FIELD, to_entity_id)
			solrDocument.setField(LinkContants.EDGE_DIRECTION_TYPE_FIELD, LinkContants.DIRECTION_UNIDIRECTIONAL)
		} else {
			id_label = resourceId.concat(ID_ELP_TYPE_SEPERATOR)
				.concat(id_1).concat(Contants.ID_SPACE_MARK)
				.concat(id_0)
			from_entity_type = elementLink.getTargetEntity
			to_entity_type = elementLink.getSourceEntity
			from_entity_id = elementLink.getTargetEntity.concat(Contants.ID_SPACE_MARK).concat(id_1)
			to_entity_id = elementLink.getSourceEntity.concat(Contants.ID_SPACE_MARK).concat(id_0)
			solrDocument.setField(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, from_entity_type)
			solrDocument.setField(LinkContants.EDGE_FROM_VERTEX_ID_FIELD, from_entity_id)
			solrDocument.setField(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, to_entity_type)
			solrDocument.setField(LinkContants.EDGE_TO_VERTEX_ID_FIELD, to_entity_id)
			solrDocument.setField(LinkContants.EDGE_DIRECTION_TYPE_FIELD, LinkContants.DIRECTION_UNIDIRECTIONAL)
		}

		solrDocument.setField(LinkContants.HBASE_TABLE_ROWKEY, id_label)

		solrDocument.setField(LinkContants.EDGE_TYPE_FIELD, elementLink.getUuid)
		solrDocument.setField(LinkContants.EDGE_ID_FIELD, id_label)
		solrDocument.setField(LinkContants.EDGE_RESID, resourceId)
		solrDocument.addField("_indexed_at_tdt", new Date())
		solrDocument
	}

	def parseLinkWithResid(resourceId: String, jsonBody: JSONObject, elementLink: Link, dbMap: ElpModelDBMapping, elp: ElpModel): JSONObject = {
		val elpJsonObj = new JSONObject()
		try {

			if (Option(elementLink.getSourceEntity).isEmpty
				|| Option(elementLink.getTargetEntity).isEmpty) {
				return null
			}

			val props = elementLink.getProperties
			val linkData = new CompactLinkData(elementLink.getUuid)

			for (prop <- props.asScala) {
				val fieldUuid = prop.getUuid
				val colName = ELPTransUtils.findColName(fieldUuid, dbMap)
				if ("" != colName) {
					val value = jsonBody.getString(colName)
					linkData.addProperty(new PropertyData(prop.getName, value))
				}
			}

			val idCols = Option(dbMap.getIdToColumns)
			val idStr = {
				val idBuilder = new StringBuilder(elementLink.getUuid)
				ELPTransUtils.parseCols(idBuilder, idCols, jsonBody)
			}
			if (Option(idStr).isEmpty) {
				return null
			}

			val resId = resourceId
			elpJsonObj.put(LinkContants.HBASE_TABLE_ROWKEY, resId + ID_ELP_TYPE_SEPERATOR + idStr.toString())

			// 判断并调整链接方向
			var dataDirectivity: Directivity = null
			var needReverseDirection = false
			// dirtected => true: 有向, false: 无向
			if (elementLink.isDirected) {
				if (dbMap.getDirectivity == Directivity.NotDirected) {
					dataDirectivity = Directivity.SourceToTarget
				} else if (Directivity.TargetToSource == dbMap.getDirectivity) {
					needReverseDirection = true
				} else if (Directivity.SourceToTarget == dbMap.getDirectivity) {
					needReverseDirection = false
				} else { // 单一列
					val directionColumn = Option(dbMap.getDirectionColumn)
					if (directionColumn.nonEmpty) {
						dataDirectivity = directionColumn.get.testLinkData(linkData,
							elementLink.getProperty(directionColumn.get.getColumnName))
						dataDirectivity = dataDirectivity match {
							case Directivity.TargetToSource =>
								needReverseDirection = true
								Directivity.SourceToTarget
							case _ =>
								dataDirectivity
						}
					}
				}

				if (Option(dataDirectivity).isEmpty) {
					dataDirectivity = if (dbMap.getDirectivity == Directivity.TargetToSource) Directivity.TargetToSource else dbMap.getDirectivity
				}

				if (!needReverseDirection) { // 不需要调整
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "source", jsonBody, dbMap))
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "target", jsonBody, dbMap))
				} else { // 调整方向
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
					elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "source", jsonBody, dbMap))
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
					elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
						parseEntityId(elementLink, "target", jsonBody, dbMap))
				}
			} else {
				dataDirectivity = Directivity.NotDirected
				elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
				elpJsonObj.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
					parseEntityId(elementLink, "source", jsonBody, dbMap))
				elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
				elpJsonObj.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
					parseEntityId(elementLink, "target", jsonBody, dbMap))
			}

			// 数据方向
			val directionType = dataDirectivity match {
				case Directivity.SourceToTarget =>
					LinkContants.DIRECTION_UNIDIRECTIONAL
				case Directivity.TargetToSource =>
					LinkContants.DIRECTION_UNIDIRECTIONAL
				case Directivity.NotDirected =>
					LinkContants.DIRECTION_UNDIRECTED
				case Directivity.Bidirectional =>
					LinkContants.DIRECTION_BIDIRECTIONAL
				case _ =>
					LinkContants.DIRECTION_UNIDIRECTIONAL
			}
			elpJsonObj.put(LinkContants.EDGE_DIRECTION_TYPE_FIELD, directionType)
			elpJsonObj.put(LinkContants.EDGE_TYPE_FIELD, elementLink.getUuid)
			elpJsonObj.put(LinkContants.EDGE_ID_FIELD, idStr.toString())
			elpJsonObj.put(LinkContants.EDGE_DSID, resId)
			elpJsonObj.put("body", getJsonBody(props, jsonBody, elementLink, dbMap))
			logDebug(s"Link elpJsonObj=${elpJsonObj}")
		} catch {
			case ex: RuntimeException => logError("=============> 解析链接数据异常", ex)
			case ex: Exception => logError("=============> 解析链接数据异常", ex)
				return null
		}
		elpJsonObj
	}


	def parseEntityId(link: Link, sType: String, jsonBody: JSONObject, dbMap: ElpModelDBMapping): String = {
		val idStr = if (LinkContants.LINK_SOURCE.equals(sType)) {
			val id = new StringBuilder(link.getSourceRootSemanticType)
			val sourceCols = Option(dbMap.getSourceColumns)
			ELPTransUtils.parseCols(id, sourceCols, jsonBody)
		} else {
			val id = new StringBuilder(link.getTargetRootSemanticType)
			val targetCols = Option(dbMap.getTargetColumns)
			ELPTransUtils.parseCols(id, targetCols, jsonBody)
		}
		if (Option(idStr).nonEmpty) {
			return idStr.toString()
		} else {
			throw new NullPointerException(s"链接两端的实体id存在空值, link uuid: ${link.getUuid}, link name: ${link.getName}, 端点: ${sType}")
		}
	}

	def getJsonBody(props: java.util.List[Property], jsonBody: JSONObject, element: PropertyBag, dbMap: ElpModelDBMapping): JSONObject = {
		val elpJsonBody = new JSONObject()
		import scala.collection.JavaConversions._
		for (p <- props) {
			val key = p.getUuid
			val colName = Option(ELPTransUtils.findColName(key, dbMap))
			if (colName.nonEmpty) {
				val pValue = Option(jsonBody.getString(colName.get))
				if (pValue.nonEmpty) {
					val dataJsonObj = new JSONObject()
					dataJsonObj.put("value", pValue.get)
					if (PropertyTypeConstants.text.equals(element.getPropertyByUUID(key).getType.toString)) {
						if (element.getPropertyByUUID(key).isAnalyse) {
							dataJsonObj.put("type", PropertyTypeConstants.text)
						} else {
							dataJsonObj.put("type", PropertyTypeConstants.string)
						}
					} else {
						dataJsonObj.put("type", element.getPropertyByUUID(key).getType.toString)
					}

					elpJsonBody.put(key, dataJsonObj)
				}
			}
		}
		elpJsonBody
	}


}
