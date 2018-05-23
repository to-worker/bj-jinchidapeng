package com.zqykj.batch.document.db

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

/**
  * @author alfer at 12/21/17
  */
class DocMongoExecutor(@transient val sc: SparkContext) extends Logging {

	val sparkConf = sc.getConf

	val mongoHost = sparkConf.get("spark.batch.mongodb.host", "172.30.6.14")

	def loadDataFromMongo(collection: String): DataFrame = {
		logInfo(s"collection: ${collection}")
		val mongoContext: SQLContext = new SQLContext(sc)
		val options = Map[String, String](
			"host" -> mongoHost,
			"database" -> sparkConf.get("spark.batch.mongodb.db", "hyjj"),
			"collection" -> collection,
			"credentials" -> "zqykj,hyjj,zqykj"
		)
		val df = mongoContext.read.format("com.stratio.datasource.mongodb").options(options).load()
		df
	}

	def getDocType(collection: String, docType: String): RDD[String] = {
		val df = loadDataFromMongo(collection)
		df.filter(df.col("docType").equalTo(docType)).toJSON
	}

	def getLastTask(collection: String, docType: String, status: String, elpModel: String): Long = {
		val df = loadDataFromMongo(collection)
		val lastDF = df.filter(df.col("docType").equalTo(docType))
			.filter(df.col("status").equalTo(status))
			.filter(df.col("elpModel").equalTo(elpModel))
			.select(df.col("lastSynTimeStamp"))
			.sort(df.col("lastSynTimeStamp").desc)
		val rows = lastDF.collect()
		if (rows.length < 1) {
			return 0
		}
		val lastSynTime = rows.head.getString(0)
		if (lastSynTime != null) {
			return lastSynTime.toLong
		} else {
			return 0
		}

	}

	def getAllResIdInFile(collection: String): mutable.HashSet[String] = {
		val set = new mutable.HashSet[String]()
		val df = loadDataFromMongo(collection)
		if (df.collect().size > 0) {
			val resIdDocs = df.select(df.col("resId"))
			val docs = resIdDocs.collect()
			docs.foreach(doc => set.add(doc.getString(0)))
			logInfo(s"Resids got from ${collection} collection contain ${set.size} size.")
			set.foreach(resid => logInfo(s"************* resid: ${resid}"))
		} else {
			logWarning(s"${collection} contains nothing resids.")
		}
		set
	}

}
