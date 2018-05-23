package com.zqykj.streaming.business

import java.util

import com.zqykj.streaming.job.{BlackWarningStreaming, SqlExecutor}
import com.zqykj.streaming.util.LoggerLevels
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by alfer on 9/16/17.
  */
object BlacklistWarningJob extends Logging {

    LoggerLevels.setStreamingLogLevels(Level.INFO)

    val sparkConf = new SparkConf()
        .setAppName("Blacklist Warning")
        .set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
        .set("spark.ui.port", "4042")

    if (sparkConf.getBoolean("spark.execute.local.model", true)) sparkConf.setMaster("local[4]")

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(sparkConf)

        val sqlExecutor = new SqlExecutor(sc)
        val phNumberblacklistBroadCast: Broadcast[util.ArrayList[String]] = sc.broadcast(sqlExecutor.getBlacklistFromMySQL())

        val blackWarningStreaming = new BlackWarningStreaming(sc, phNumberblacklistBroadCast)
        blackWarningStreaming.execute()

    }

}
