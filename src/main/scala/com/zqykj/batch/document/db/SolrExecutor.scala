package com.zqykj.batch.document.db


import com.lucidworks.spark.SolrSupport
import com.zqykj.batch.document.entity.DocType
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import com.zqykj.streaming.common.Contants.DocEntityConstants
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.SystemDefaultHttpClient
import org.apache.solr.client.solrj.impl.CloudSolrClient

/**
  * @author alfer
  * @date 12/22/17
  */
class SolrExecutor(@transient sc: SparkContext) extends Logging with Serializable {

	val solrZkServer = sc.getConf.get("spark.batch.solr.zk.server",
		"zqykjdev14:2181/solr")
	println(s"spark.batch.solr.zk.server=${sc.getConf.get("spark.batch.solr.zk.server", "zqykjdev14:2181/solr")}")
	val docCollection = sc.getConf.get("spark.batch.doc.collection", "doc_card_index_info")
	val batchSize = sc.getConf.getInt("spark.batch.doc.solr.size", 10000)

	def save(rdd: RDD[SolrInputDocument]): Unit = {
		// logInfo(s"begin write docs to solr, doc count=${rdd.count()}, rdd.partitions.size=${rdd.partitions.length}")
		if (!rdd.isEmpty()) {
			rdd.foreachPartition(fp => {
				if (fp.nonEmpty) {
//					val httpClient: HttpClient = new SystemDefaultHttpClient()
//					val solrClient = new CloudSolrClient(solrZkServer, httpClient)
//					val batch = new java.util.ArrayList[SolrInputDocument]
//					fp.foreach(f => {
//						batch.add(f)
//						if (batch.size() > batchSize) {
//							SolrSupport.sendBatchToSolr(solrClient, docCollection, batch)
//						}
//					})
//					if (batch.size() > 0) {
//						SolrSupport.sendBatchToSolr(solrClient, docCollection, batch)
//					}
//					solrClient.close()
				}
			})
		}
	}

	def buildDoc(docType: String, rdd: RDD[(String, ArrayBuffer[(String, String)])]): RDD[SolrInputDocument] = {
		rdd.map(r => {
			val doc = new SolrInputDocument()
			doc.setField(DocEntityConstants.DOC_ENTITY_DOC_TYPE, docType)
			doc.setField(DocEntityConstants.DOC_ENTITY_DOC_KEY, r._1)
			doc.setField(DocEntityConstants.DOC_ENTITY_ID,
				docType.concat(DocEntityConstants.DOC_ENTITY_ID_KEY_SEPERATOR).concat(r._1))

			if (DocType.PEOPLE.toString.equals(docType)) {
				r._2.foreach(kv => {
					// 只要在重点人员表中存在就认为是重点人员
					if ("tab_s_showTag".equals(kv._1) && kv._2.nonEmpty) {
						doc.setField(kv._1, DocEntityConstants.DOC_ENTITY_PEOPLE_SHOW_TAG)
					} else {
						doc.addField(kv._1, kv._2)
					}
				})
			} else {
				r._2.foreach(kv => {
					doc.addField(kv._1, kv._2)
				})
			}

			doc
		})
	}

}
