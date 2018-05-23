package com.zqykj.solr

import java.util

import com.lucidworks.spark.SolrSupport
import com.zqykj.batch.transform.db.SolrLoadExecutor
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.SystemDefaultHttpClient
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * @author alfer
  * @date 2/2/18
  */
class SolrTest {

	@Test
	def testSolrField(): Unit = {
		val zkHost = "172.30.6.14"
		val zkCHroot = "/solrv7test"
		val solrClientServer = new CloudSolrClient.Builder()
			.withZkHost(zkHost)
			.withZkChroot(zkCHroot)
			.build()
		solrClientServer.setDefaultCollection("test14_1")
		//solrClientServer.add(getSolrInputDoc())
		solrClientServer.add(getSolrInputs())
		solrClientServer.commit()
		solrClientServer.close()
	}

	@Test
	def testHttpSolrClient(): Unit = {
		val solrUrl = "http://172.30.6.14:9983/solr"
		val httpSolrClient = new HttpSolrClient.Builder()
			.withBaseSolrUrl(solrUrl)
			.withConnectionTimeout(10000)
			.withSocketTimeout(6000)
			.build()

		httpSolrClient.add(getSolrInputDoc())
		httpSolrClient.commit("test14_1")
	}

	def getSolrInputDoc(): SolrInputDocument = {
		val document = new SolrInputDocument()
		document.setField("name", "zhaoliu")
		document.setField("age", 30)
		document
	}

	def getSolrInputs(): java.util.Collection[SolrInputDocument] = {
		val cols = new util.ArrayList[SolrInputDocument]()
		cols.add(getSolrInputDoc())
		cols
	}

	@Test
	def testSpark2Solr(): Unit = {
		val sparConf = new SparkConf()
			.setMaster("local[4]")
			.setAppName("test")
		val sc = new SparkContext(sparConf)
		val solrLoadExecutor = new SolrLoadExecutor(sc)
		val data = List(getSolrInputDoc())
		val rdd = sc.parallelize(data)
		solrLoadExecutor.indexDocs("test14_1", rdd)
		sc.stop()
	}

}
