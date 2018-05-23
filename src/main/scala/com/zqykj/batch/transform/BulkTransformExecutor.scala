package com.zqykj.batch.transform

import com.zqykj.hyjj.entity.elp._
import com.zqykj.streaming.common.Contants._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject
import com.zqykj.batch.transform.db.{HBasePutExecutor, SolrLoadExecutor}
import com.zqykj.hyjj.query.{CompactLinkData, PropertyData}
import com.zqykj.streaming.common.{Contants, JobConstants}
import com.zqykj.streaming.util.{DistIdUtils, ELPTransUtils}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConverters._

/**
  * @author alfer
  * @date 1/22/18
  */
class BulkTransformExecutor(@transient val sc: SparkContext,
                            val dsIds: mutable.HashSet[String],
                            val sequenceId: Long,
                            val elpModelBroadcast: Broadcast[mutable.HashMap[String, ElpModel]],
                            val elpDBMappingBroadcast: Broadcast[mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]],
                            val elpModelAndMappingBroadcast: Broadcast[mutable.HashMap[String, PropertyBag]]) extends Logging with Serializable {

	val elpAndMappingsCache = elpModelAndMappingBroadcast.value

	val basePath = sc.getConf.get("spark.trans.data.path", "/outer/172.30.6.25/extractormanager/backup")
	val baseCompactPath = basePath + "/" + "compact/"
	val baseReadPath = basePath + "/" + "read/"
	logInfo(s"=======> basePath: ${basePath}")
	logInfo(s"=======>  compactPath: ${baseCompactPath}")

	val zkQurom = sc.getConf.get("spark.kafka.stream.transform.zkqurom", "skynettest01:2181")

	val hBasePutExecutor = new HBasePutExecutor(sc)

	val solrLoadExecutor = new SolrLoadExecutor(sc)

	def getSeqIdsByFileName(files: Array[FileStatus]): mutable.HashSet[Long] = {
		val seqidSet = new mutable.HashSet[Long]()
		for (file: FileStatus <- files) {
			val fileName = file.getPath.getName
			if (fileName.endsWith(JobConstants.avroExt)) {
				seqidSet.add(fileName.substring(0, fileName.indexOf(JobConstants.avroExt)).toLong)
			}
		}
		seqidSet
	}

	def filterFileBySequenceId(dirPath: String): mutable.HashSet[String] = {
		logInfo(s"=======> filterFileBySequenceId dirPath: ${dirPath}")
		val avroPaths = new mutable.HashSet[String]()
		val hadoopConfiguration = sc.hadoopConfiguration
		if (sc.getConf.getBoolean("spark.execute.local.model", true))
			hadoopConfiguration.set(FileSystem.FS_DEFAULT_NAME_KEY, sc.getConf.get("spark.trans.fs.defaultFS", "hdfs://skynettest01:8020"))

		val fileSystem = FileSystem.get(sc.hadoopConfiguration)
		val path = new Path(dirPath)
		if (!fileSystem.exists(path)) {
			return null
		}

		val files = fileSystem.listStatus(path)
		logInfo(s"=======> dirPath: ${dirPath}, files.size: ${files.size}")
		if (files.size <= 0) {
			return avroPaths
		}
		DistIdUtils.apply(zkQurom)
		val seqids = DistIdUtils.getEffectSeqIds(
			DistIdUtils.sortSeqIds(getSeqIdsByFileName(files).toList)
			, sequenceId)

		val names = seqids.map(m => m + JobConstants.avroExt) // .reduce((a,b) => a)
		for (file: FileStatus <- files) {
			if (file.isFile) {
				val fileName = file.getPath.getName
				logInfo(s"=======> fileName: ${fileName}")
				if (names.contains(fileName)) {
					logInfo(s"===========> fileName: ${fileName}, add avroPath: ${file.getPath.toString}")
					avroPaths.add(file.getPath.toString)
				}
			} else {
				logWarning(s"=======> find dir: ${file.getPath.toString}")
			}
		}
		avroPaths
	}

	def readFile(dsId: String, path: String): RDD[Row] = {
		val dirPath = path + dsId
		logInfo(s"=======> dsId: ${dsId}, path: ${basePath}, dirPath: ${dirPath}")
		val filteredFilePaths = filterFileBySequenceId(dirPath)
		if (Option(filteredFilePaths).isEmpty || filteredFilePaths.size <= 0) {
			logWarning(s"=======> dirPath: ${dirPath} is not exist or the dirPath has no file.")
			return null
		}
		val sqlContext = new SQLContext(sc)
		val rdds = new mutable.HashSet[RDD[Row]]()
		filteredFilePaths.foreach(filePath => {
			val avroRDD = sqlContext.read.format("com.databricks.spark.avro").load(filePath).rdd
			rdds.add(avroRDD)
		})
		sc.union(rdds.toArray)
	}

	def readAndCompactedFiles(dsId: String): RDD[Row] = {
		val baseRDD = readFile(dsId, baseReadPath)
		val compactRDD = readFile(dsId, baseCompactPath)
		if (Option(baseRDD).isEmpty) {
			compactRDD
		} else if (Option(compactRDD).isEmpty) {
			baseRDD
		} else {
			baseRDD.union(compactRDD)
		}
	}

	def readAvroFile(dsId: String): DataFrame = {
		val avroFilePath = basePath + dsId + Contants.SEPERATOR + JobConstants.wildCardAvroExt
		val sqlContext = new SQLContext(sc)
		val avroDF = sqlContext.read.format("com.databricks.spark.avro").load(avroFilePath)
		avroDF
	}

	def getPropsValue(row: Row, propertyBag: PropertyBag, dbMap: ElpModelDBMapping): JSONObject = {
		val bodyObj = new JSONObject()
		val props: java.util.List[Property] = propertyBag.getProperties
		for (p <- props.asScala) {
			val key = p.getUuid
			val colName = Option(ELPTransUtils.findColName(key, dbMap))
			if (colName.nonEmpty && !"".equals(colName.get)) {
				val pValue = Option(row.get(row.fieldIndex(colName.get)))
				if (pValue.nonEmpty) {
					val dataJsonObj = new JSONObject()
					dataJsonObj.put("value", pValue.get)
					dataJsonObj.put("type", propertyBag.getPropertyByUUID(key).getType.toString)
					bodyObj.put(key, dataJsonObj)
				}
			}
		}
		bodyObj
	}

	private def parseId(idStr: StringBuilder, row: Row, dbMap: ElpModelDBMapping): String = {
		val idCols = Option(dbMap.getIdToColumns)
		// 1: idCols,  2 : idCol 如果标识属性值为空, 则过滤此条记录
		if (idCols.nonEmpty) {
			for (idCol <- idCols.get.asScala) {
				val value = Option(row.get(row.fieldIndex(idCol)))
				if (!value.isEmpty) {
					if (idStr.length > 0) idStr.append(Contants.ID_SPACE_MARK)
					idStr.append(value.get)
				} else {
					return null
				}
			}
		} else {
			return null
		}
		idStr.toString
	}

	def parseEntity(row: Row, element: JSONObject, dbMap: ElpModelDBMapping, elementEntity: Entity): JSONObject = {
		val idStr = new StringBuilder(elementEntity.getRootSemanticType)
		val idOption = Option(parseId(idStr, row, dbMap))
		val id = if (idOption.nonEmpty) {
			idOption.get
		} else {
			return null
		}

		val resId = row.getString(row.fieldIndex(EntityConstants.VERTEXT_RESID_UPPER))
		val owner = row.getString(row.fieldIndex(EntityConstants.VERTEXT_OWNER))
		//		val resId = "testResId"
		//		val owner = "testOwner"
		element.put(EntityConstants.HBASE_TABLE_ROWKEY, resId + ID_ELP_TYPE_SEPERATOR + id)
		element.put(EntityConstants.VERTEXT_RESID, resId)
		element.put(EntityConstants.VERTEXT_OWNER, owner)
		element.put(EntityConstants.VERTEX_ID_FILED, idStr.toString)
		element.put(EntityConstants.VERTEX_TYPE_FILED, elementEntity.getUuid)

		element
	}

	def parseLink(row: Row, element: JSONObject, dbMap: ElpModelDBMapping, elementLink: Link, elp: ElpModel): JSONObject = {
		val props = elementLink.getProperties
		val linkData = new CompactLinkData(elementLink.getUuid)
		val idStr = new StringBuilder(elementLink.getUuid)
		for (prop <- props.asScala) {
			val fieldUuid = prop.getUuid
			val colName = Option(ELPTransUtils.findColName(fieldUuid, dbMap))
			if (colName.nonEmpty) {
				val value = row.get(row.fieldIndex(colName.get))
				linkData.addProperty(new PropertyData(prop.getName, value))
			}
		}

		val idCols = Option(dbMap.getIdToColumns)
		if (idCols.nonEmpty) {
			for (idCol <- idCols.get.asScala) {
				val value = Option(row.get(row.fieldIndex(idCol)))
				if (!value.isEmpty) {
					if (idStr.length > 0) idStr.append(Contants.ID_SPACE_MARK)
					idStr.append(value.get)
				} else {
					return null
				}
			}
		} else {
			return null
		}

		val resId = Option(row.get(row.fieldIndex(LinkContants.EDGE_RESID_UPPER))).getOrElse("testLinkResId")
		val owner = Option(row.get(row.fieldIndex(LinkContants.EDGE_OWNER))).getOrElse("testLinkOwner")
		element.put(LinkContants.HBASE_TABLE_ROWKEY, resId + ID_ELP_TYPE_SEPERATOR + idStr.toString())

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

			if (dataDirectivity == null) {
				dataDirectivity = if (dbMap.getDirectivity == Directivity.TargetToSource) Directivity.TargetToSource else dbMap.getDirectivity
			}

			if (!needReverseDirection) { // 不需要调整
				element.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
				element.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
					parseEntityId(row, elementLink, "source", elp.getEntityByUuid(elementLink.getSourceRootSemanticType), dbMap))
				element.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
				element.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
					parseEntityId(row, elementLink, "target", elp.getEntityByUuid(elementLink.getTargetRootSemanticType), dbMap))
			} else { // 调整方向
				element.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetRootSemanticType)
				element.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
					parseEntityId(row, elementLink, "source", elp.getEntityByUuid(elementLink.getTargetRootSemanticType), dbMap))
				element.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
				element.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
					parseEntityId(row, elementLink, "target", elp.getEntityByUuid(elementLink.getSourceRootSemanticType), dbMap))
			}
		} else {
			dataDirectivity = Directivity.NotDirected
			element.put(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, elementLink.getSourceEntity)
			element.put(LinkContants.EDGE_FROM_VERTEX_ID_FIELD,
				parseEntityId(row, elementLink, "source", elp.getEntityByUuid(elementLink.getSourceRootSemanticType), dbMap))
			element.put(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, elementLink.getTargetEntity)
			element.put(LinkContants.EDGE_TO_VERTEX_ID_FIELD,
				parseEntityId(row, elementLink, "target", elp.getEntityByUuid(elementLink.getTargetRootSemanticType), dbMap))
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
		element.put(LinkContants.EDGE_DIRECTION_TYPE_FIELD, directionType)
		element.put(LinkContants.EDGE_TYPE_FIELD, elementLink.getUuid)
		element.put(LinkContants.EDGE_ID_FIELD, idStr.toString())
		element.put(LinkContants.EDGE_RESID, resId)
		element.put(LinkContants.EDGE_OWNER, owner)
		logDebug(s"===========> Link element=${element}")

		element
	}

	def parseEntityId(row: Row, link: Link, sType: String, foreign: Entity, dbMap: ElpModelDBMapping): String = {
		val idStr = if (LinkContants.LINK_SOURCE.equals(sType)) {
			new StringBuilder(link.getSourceRootSemanticType)
		} else {
			new StringBuilder(link.getTargetRootSemanticType)
		}
		val sourceCols = Option(dbMap.getSourceColumns)
		if (sourceCols.nonEmpty) {
			for (sCol <- sourceCols.get.asScala) {
				val colValue = Option(row.get(row.fieldIndex(sCol)))
				if (colValue.nonEmpty) {
					if (idStr.length > 0) idStr.append(Contants.ID_SPACE_MARK)
					idStr.append(colValue.get)
				}
			}
		}
		idStr.toString()
	}

	def dataTransform2(dBMappings: Option[ArrayBuffer[ElpModelDBMapping]], rowRDD: RDD[Row]): ArrayBuffer[RDD[(String, JSONObject)]] = {
		if (dBMappings.isEmpty) {
			return null
		}
		val elpDataWithKey = dBMappings.get.map(mapping => {
			val elpModel = elpModelBroadcast.value.get(mapping.getElp)
			val key = if (PropertyBag.Type.Entity.equals(mapping.getElpTypeDesc)) {
				mapping.getElp + ELP_MAPPING_SEPARATOR + EntityConstants.ELP_ENTITY + ELP_MAPPING_SEPARATOR + mapping.getElpType
			} else if (PropertyBag.Type.Link.equals(mapping.getElpTypeDesc)) {
				mapping.getElp + ELP_MAPPING_SEPARATOR + LinkContants.ELP_LINK + ELP_MAPPING_SEPARATOR + mapping.getElpType
			} else {
				null
			}
			val elpMapKey = mapping.getElp + ELP_MAPPING_SEPARATOR + mapping.getElpType + ELP_MAPPING_SEPARATOR + mapping.getElpTypeDesc.toString
			val property = elpAndMappingsCache.get(elpMapKey)
			val mappingData = rowRDD.map(row => {
				var elementJSONObj = new JSONObject()
				// elp_elpType_Entity
				if (property.nonEmpty) {
					val bodyObj = getPropsValue(row, property.get, mapping)
					elementJSONObj.put("body", bodyObj)

					if (PropertyBag.Type.Entity.equals(mapping.getElpTypeDesc)) {
						// (elpId_entity_elpType, elpEntityData)
						elementJSONObj = parseEntity(row, elementJSONObj, mapping, property.get.asInstanceOf[Entity])
					} else if (PropertyBag.Type.Link.equals(mapping.getElpTypeDesc)) {
						// (elpId_relation_elpType, elpLinkData)
						elementJSONObj = parseLink(row, elementJSONObj, mapping, property.get.asInstanceOf[Link], elpModel.get)
					}
				}
				(key, elementJSONObj)
			})
			// data to heavy
			mappingData.map(md => (md._1.concat(md._2.getString(EntityConstants.HBASE_TABLE_ROWKEY)), md._2))
				.reduceByKey((a, b) => b)
				.filter(f => !f._2.isEmpty)
		})
		elpDataWithKey
	}

	def persist(rdd: RDD[(String, JSONObject)]): Unit = {
		if (!rdd.isEmpty()) {
			val tableName = rdd.first()._1
			val vRDD = rdd.map(m => m._2)
			if (tableName.contains(EntityConstants.ELP_ENTITY)) {
				hBasePutExecutor.bulkPut(vRDD, tableName, hBasePutExecutor.compactEntityPut)
				val solrCollection = ELPTransUtils.getCollectionNameByTableName(tableName, Contants.ENTITY)
				solrLoadExecutor.indexDocs(Contants.ENTITY, solrCollection, vRDD)
			} else if (tableName.contains(LinkContants.ELP_LINK)) {
				hBasePutExecutor.bulkPut(vRDD, tableName, hBasePutExecutor.compactLinkPut)
				val solrCollection = ELPTransUtils.getCollectionNameByTableName(tableName, Contants.LINK)
				solrLoadExecutor.indexDocs(Contants.LINK, solrCollection, vRDD)
			}
		}
	}

	/**
	  * 根据唯一id去重
	  *
	  * @param rdd
	  * @return
	  */
	def removeDuplicatesById(rdd: RDD[(String, JSONObject)]): RDD[(String, JSONObject)] = {
		rdd.map(m => {
			val id = m._2.getString(EntityConstants.HBASE_TABLE_ROWKEY)
			(id, m)
		}).reduceByKey((x, y) => x)
			.map(m => m._2)
	}

	def execute(): Unit = {

		for (dsId <- dsIds) {
			// 1 根据dataSchemaId读取原始文件
			val avroRDD = readAndCompactedFiles(dsId)
			if (!Option(avroRDD).isEmpty) {

				// 2 获取ELPDBMapping (elp_entity_elpType, ?)
				val dBMappings = elpDBMappingBroadcast.value.get(dsId)
				if (!dBMappings.isEmpty) {
					// 3 按照映射分类转换: ArrayBuffer[RDD[(String, HashMap[Strig, Any]]]
					val elpDataRDDs = dataTransform2(dBMappings, avroRDD)
					elpDataRDDs.foreach(rdd => {
						// 根据唯一id去重
						val distinctRDD = removeDuplicatesById(rdd)

						// 持久化
						persist(distinctRDD)
					})
				}
			}

		}
	}

}
