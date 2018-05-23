package com.zqykj.batch.transform.db

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zqykj.batch.document.db.SolrClient2Support
import com.zqykj.streaming.common.{Contants, JobPropertyConstant}
import com.zqykj.streaming.common.Contants.{EntityConstants, LinkContants, PropertyTypeConstants}
import com.zqykj.streaming.util.TypeConvertUtils
import com.zqykj.tldw.util.HBaseUtils
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

/**
  * @author alfer
  * @date 1/24/18
  */
class SolrLoadExecutor(@transient sc: SparkContext) extends Logging with Serializable {

	val zkHost = sc.getConf.get("spark.load.solr.zk.server",
		JobPropertyConstant.ZOOKEEPER_SERVER_DEFAULT)
	val zkChroot = sc.getConf.get("spark.solr.zk.chroot",JobPropertyConstant.ZOOKEEPER_CH_ROOT_DEFAULT)
	val batchSize = sc.getConf.getInt("spark.trans.solr.batch.size", JobPropertyConstant.SOLR_BATCH_SIZE_DEFAULT)

	def docBody(jsonObj: JSONObject, solrDocument: SolrInputDocument): SolrInputDocument = {
		val jsonBody = jsonObj.getJSONObject("body")
		val bodyIter = jsonBody.entrySet().iterator()
		while (bodyIter.hasNext) {
			val element = bodyIter.next()
			val key = element.getKey
			val value = element.getValue.toString
			val fieldValue = convertType(key, value)
			if (!"".equals(fieldValue._1)) {
				solrDocument.addField(fieldValue._1, fieldValue._2)
			}
		}
		solrDocument
	}

	def convertType(key: String, value: String): (String, Object) = {
		val valueAndType = JSON.parseObject(value)
		val fieldType = valueAndType.get("type")
		val fieldValue: String = valueAndType.get("value").toString

		val kv = fieldType match {
			case PropertyTypeConstants.bool => ("g_bn11_".concat(key), fieldValue)
			case PropertyTypeConstants.date => ("g_rd11_".concat(key), TypeConvertUtils.getTypeConvert("date", fieldValue))
			case PropertyTypeConstants.datetime => ("g_rd11_".concat(key), TypeConvertUtils.getTypeConvert("datetime", fieldValue))
			// case PropertyTypeConstants.datetime => ("g_rd11_".concat(key), new Date())
			case PropertyTypeConstants.time => ("g_tk11_".concat(key), fieldValue)
			case PropertyTypeConstants.integer => ("g_it11_".concat(key), fieldValue)
			case PropertyTypeConstants.number => ("g_de11_".concat(key), TypeConvertUtils.getTypeConvert("number", fieldValue))
			case PropertyTypeConstants.text => ("g_tt11_".concat(key), fieldValue)
			case PropertyTypeConstants.string => ("g_tk11_".concat(key), fieldValue)
			case _ => {
				logWarning(s"=============> key: ${key} 没有匹配到任何类型")
				("", "")
			}
		}
		kv
	}

	def compactEntityDoc(jsonObj: JSONObject): SolrInputDocument = {
		val solrDocument = new SolrInputDocument()
		solrDocument.addField(EntityConstants.HBASE_TABLE_ID, HBaseUtils.md5Hash(jsonObj.getString(EntityConstants.HBASE_TABLE_ROWKEY)))
		solrDocument.addField(EntityConstants.VERTEX_ID_FILED, jsonObj.getString(EntityConstants.VERTEX_ID_FILED))
		solrDocument.addField(EntityConstants.VERTEX_TYPE_FILED, jsonObj.getString(EntityConstants.VERTEX_TYPE_FILED))
		solrDocument.addField(EntityConstants.VERTEXT_RESID, jsonObj.getString(EntityConstants.VERTEXT_RESID))
		solrDocument.addField(EntityConstants.VERTEXT_OWNER, jsonObj.getString(EntityConstants.VERTEXT_OWNER))
		docBody(jsonObj, solrDocument)
	}

	def compactLinkDoc(jsonObj: JSONObject): SolrInputDocument = {
		val solrDocument = new SolrInputDocument()
		solrDocument.addField(LinkContants.HBASE_TABLE_ID, HBaseUtils.md5Hash(jsonObj.getString(LinkContants.HBASE_TABLE_ROWKEY)))
		solrDocument.addField(LinkContants.EDGE_ID_FIELD, jsonObj.getString(LinkContants.EDGE_ID_FIELD))
		solrDocument.addField(LinkContants.EDGE_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_TYPE_FIELD))
		solrDocument.addField(LinkContants.EDGE_DIRECTION_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_DIRECTION_TYPE_FIELD))
		solrDocument.addField(LinkContants.EDGE_FROM_VERTEX_ID_FIELD, jsonObj.getString(LinkContants.EDGE_FROM_VERTEX_ID_FIELD))
		solrDocument.addField(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD))
		solrDocument.addField(LinkContants.EDGE_TO_VERTEX_ID_FIELD, jsonObj.getString(LinkContants.EDGE_TO_VERTEX_ID_FIELD))
		solrDocument.addField(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD, jsonObj.getString(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD))
		solrDocument.addField(LinkContants.EDGE_RESID, jsonObj.getString(LinkContants.EDGE_RESID))
		solrDocument.addField(LinkContants.EDGE_OWNER, jsonObj.getString(LinkContants.EDGE_OWNER))

		docBody(jsonObj, solrDocument)
	}

	def buildEntityDocs(rdd: RDD[JSONObject]): RDD[SolrInputDocument] = {
		rdd.map(m => {
			compactEntityDoc(m)
		})
	}

	def buildLinkDocs(rdd: RDD[JSONObject]): RDD[SolrInputDocument] = {
		rdd.map(m => {
			compactLinkDoc(m)
		})
	}

	/**
	  * RemoteSolrException
	  *
	  * @param elpType
	  * @param collection
	  * @param rdd
	  */
	def indexDocs(elpType: String, collection: String, rdd: RDD[JSONObject]): Unit = {
		try {
			if (Contants.ENTITY.equals(elpType)) {
				indexDocs(collection, buildEntityDocs(rdd))
			} else if (Contants.LINK.equals(elpType)) {
				indexDocs(collection, buildLinkDocs(rdd))
			}
		} catch {
			case ex: RemoteSolrException => logError(s"=============> catch RemoteSolrException: elpType: ${elpType}, collection: ${collection}", ex)
			case ex: SolrException => logError(s"=============> catch RemoteSolrException: elpType: ${elpType}, collection: ${collection}", ex)
			case e: Exception => logError(s"=============> catch Exception: elpType: ${elpType}, collection: ${collection}", e)
		}

	}

	def indexDocs(collection: String, docsRDD: RDD[SolrInputDocument]): Unit = {
		logInfo(s"=======> zkHost:${zkHost}, zkChroot:${zkChroot}, batch:${batchSize}")
		SolrClient2Support.indexDocs(zkHost, zkChroot, collection, batchSize, docsRDD)
	}
}
