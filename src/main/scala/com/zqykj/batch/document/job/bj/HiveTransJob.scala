package com.zqykj.batch.document.job.bj

import com.zqykj.streaming.common.{Contants, JobPropertyConstant}
import com.zqykj.streaming.job.SqlExecutor
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * @author feng.wei
  * @date 2018/5/22
  */
object HiveTransJob extends Logging with Serializable {

	val sparkConf = new SparkConf()
	sparkConf.setAppName("DataTransformJob")
		.set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")

	if (sparkConf.getBoolean("spark.execute.local.model", true)) {
		sparkConf.setMaster("local[4]")
	}

	val options = Map[String, String](
		"host" -> sparkConf.get("spark.streaming.mongodb.host", JobPropertyConstant.MONGODB_HOST_DEFAULT),
		"database" -> sparkConf.get("spark.streaming.mongodb.db", "hyjj")
		, "user" -> sparkConf.get("spark.streaming.mongodb.db.user", "zqykj"),
		"password" -> sparkConf.get("spark.streaming.mongodb.db.password", "zqykj")
	)

	val elpModelId = sparkConf.get("spark.trans.elp.modelId", "standard_model")

	def main(args: Array[String]): Unit = {

		val sc = new SparkContext(sparkConf)
		val sqlExecutor = new SqlExecutor(sc)

		val elpDBMappingByELPBroad = sc.broadcast(sqlExecutor.cacheElpDBMappings(sqlExecutor.loadData(Contants.ELP_MODEL_DB_MAPPING)))
		val elpModelCache = sqlExecutor.cacheElpModel(sqlExecutor.loadData(Contants.ELP_MODELS), elpModelId)
		val elpModelCacheBroadcast = sc.broadcast(elpModelCache)
		val elpModelAndMappingsBroadcast = sc.broadcast(sqlExecutor.cacheElpEntitiesAndLinks(elpModelCache))

		val transExecutor = new HiveTransExecutor(sc,
			elpModelCacheBroadcast, elpDBMappingByELPBroad, elpModelAndMappingsBroadcast)
		transExecutor.execute()

	}

}
