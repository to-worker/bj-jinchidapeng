package com.zqykj.batch.document.db

import java.net.{ConnectException, SocketException}
import java.util
import java.util.{ArrayList, Collection, Date, Iterator, List}

import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
  * @author feng.wei
  * @date 2018/5/23
  */
object SolrClient2Support extends Logging with Serializable {

	def getSolrServer(key: String, zkChroot: String, collection: String): CloudSolrClient = {
		val solr = new CloudSolrClient.Builder().sendDirectUpdatesToShardLeadersOnly.withZkHost(key).withZkChroot(zkChroot).build
		solr.setZkClientTimeout(15 * 1000)
		solr.setParallelCacheRefreshes(12)
		solr.setDefaultCollection(collection)
		solr
	}

	def indexDocs(zkHost: String, zkChroot: String, collection: String, batchSize: Int, docs: RDD[SolrInputDocument]): Unit = {
		logInfo(s"=======> indexDocs zkHost:${zkHost}, zkChroot:${zkChroot}, batch:${batchSize}")
		docs.foreachPartition(solrInputDocumentIterator => {
			try {
				logInfo("=======> beginning solrInputDocumentIterator foreach：indexDocs zkHost:${zkHost}, zkChroot:${zkChroot}, batch:${batchSize}")
				val solrServer = getSolrServer(zkHost, zkChroot, collection)
				val batch = new java.util.ArrayList[SolrInputDocument]
				val indexedAt = new Date
				while ( {
					solrInputDocumentIterator.hasNext
				}) {
					val inputDoc = solrInputDocumentIterator.next
					inputDoc.addField("_indexed_at_tdt", indexedAt)
					batch.add(inputDoc)
					if (batch.size >= batchSize) sendBatchToSolr(solrServer, collection, batch)
				}
				if (!batch.isEmpty) sendBatchToSolr(solrServer, collection, batch)
				solrServer.close()
			}catch {
				case ex:Exception => {
					logError(s"=======> indexDocs foreachPartition has error: ${ex.getStackTraceString}")
					throw new Exception(ex.getMessage, ex)
				}
			}

		})
	}

	def sendBatchToSolr(cloudSolrClient: CloudSolrClient, collection: String, batch: java.util.Collection[SolrInputDocument]): Unit = {
		try {
			log.info("=======> write {} data to collection: {}", batch.size, collection)
			cloudSolrClient.add(batch)
			log.info("=======> write successful.")
		} catch {
			case e: Exception =>
				if (shouldRetry(e)) {
					log.error("=======> Send batch to collection " + collection + " failed due to " + e + "; will retry ...")
					try {
						Thread.sleep(2000)
					} catch {
						case e1: InterruptedException => {
							logError(s"InterruptedException: ${e1.getStackTraceString}", e1)
							Thread.interrupted
						}
					}
					try
						cloudSolrClient.add(batch)
					catch {
						case e1: Exception =>
							log.error("=======> Retry send batch to collection " + collection + " failed due to: " + e1, e1)
					}
				}
				else log.error("=======> Send batch to collection " + collection + " failed due to: " + e, e)
		} finally {
			batch.clear()
		}
	}

	private def shouldRetry(exc: Exception) = {
		val rootCause = SolrException.getRootCause(exc)
		rootCause.isInstanceOf[ConnectException] || rootCause.isInstanceOf[SocketException]
	}

}
