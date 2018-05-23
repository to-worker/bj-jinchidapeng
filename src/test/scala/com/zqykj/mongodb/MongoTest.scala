package com.zqykj.mongodb

import java.util.Date

import com.zqykj.streaming.business.LoadJob.sparkConf
import com.zqykj.streaming.metadata.{ELpModifyMap, ELpTypeModifyMap}
import com.zqykj.streaming.service.LoadMongoService
import com.zqykj.streaming.util.DistIdUtils
import org.junit.Test

/**
  * @author alfer
  * @date 1/26/18
  */
class MongoTest {

    val options = Map[String, String](
        "host" -> sparkConf.get("spark.streaming.mongodb.host", "172.30.6.61"),
        "database" -> sparkConf.get("spark.streaming.mongodb.db", "hyjj"),
        "user" -> sparkConf.get("spark.streaming.mongodb.db.user", "zqykj"),
        "password" -> sparkConf.get("spark.streaming.mongodb.db.password", "zqykj")
    )

    @Test
    def testMongoService(): Unit = {
        val loadMongoService = new LoadMongoService(options)
        val reMap = loadMongoService.getEntitiesAndLinksByElpId("standard_model", "ELPMStreamLoadEntitiesAndLinks")
        reMap.foreach(f => println(s"${f._1} ===> ${f._2}"))
    }

    @Test
    def testSave(): Unit = {
        val loadMongoService = new LoadMongoService(options)
        val seqid = DistIdUtils.getSequenceId()
        val elpMap = new ELpModifyMap
        elpMap.setElp("standard_model")
        val entities = new java.util.ArrayList[ELpTypeModifyMap]()
        val e1 = new ELpTypeModifyMap()
        e1.setElpTypeId("phone_number")
        e1.setUpdateId("1231231")
        entities.add(e1)
        elpMap.setEntities(entities)
        elpMap.setSeqId(111l)
        elpMap.setCreateTime(new Date())
        elpMap.setUpdateTime(new Date())
        loadMongoService.saveElpMap(seqid, elpMap, "ELPMTransEntitiesAndLinks")
    }

    @Test
    def testUpdate(): Unit = {
        val loadMongoService = new LoadMongoService(options)
        val seqid = DistIdUtils.getSequenceId()
        val elpMap = new ELpModifyMap
        elpMap.setElp("standard_model")
        val entities = new java.util.ArrayList[ELpTypeModifyMap]()
        val e1 = new ELpTypeModifyMap()
        e1.setElpTypeId("phone_number")
        e1.setUpdateId("0022")
        entities.add(e1)
        elpMap.setEntities(entities)
        elpMap.setSeqId(111l)
        elpMap.setCreateTime(new Date())
        elpMap.setUpdateTime(new Date())
        loadMongoService.updateElpMap(elpMap, "ELPMTransEntitiesAndLinks")
    }

}
