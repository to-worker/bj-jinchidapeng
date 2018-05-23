package com.zqykj.streaming.service

import com.alibaba.fastjson.JSON
import com.zqykj.hyjj.entity.elp.ElpModelDBMapping
import com.zqykj.streaming.common.Contants
import com.zqykj.streaming.common.Contants.{EntityConstants, LinkContants}
import com.zqykj.streaming.dao.LoadMongoDao
import com.zqykj.streaming.metadata.ELpModifyMap
import org.apache.spark.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author alfer
  * @date 1/26/18
  */
class LoadMongoService(val options: Map[String, String]) extends Logging {

    val loadMongoDao = new LoadMongoDao(options)

    def isExistByElp(elpId: String, collection: String): Boolean = {
        loadMongoDao.isExistByElp(elpId, collection)
    }

    def getEntitiesAndLinksByElpId(elpId: String, collection: String): mutable.HashMap[String, String] = {
        val map = new java.util.HashMap[String, Object]()
        val reMap = new mutable.HashMap[String, String]()
        map.put("elp", "standard_model")
        val elpModifyObj: ELpModifyMap = loadMongoDao.getOne(map, collection)
        // mapping.getElp + ELP_MAPPING_SEPARATOR + LinkContants.ELP_LINK + ELP_MAPPING_SEPARATOR + mapping.getElpType
        // mapping.getElp + ELP_MAPPING_SEPARATOR + EntityConstants.ELP_ENTITY + ELP_MAPPING_SEPARATOR + mapping.getElpType
        if (Option(elpModifyObj).nonEmpty) {
            if (Option(elpModifyObj.getEntities).nonEmpty) {
                for (entity <- elpModifyObj.getEntities.asScala) {
                    reMap.put(elpId.concat(Contants.ELP_MAPPING_SEPARATOR)
                        .concat(EntityConstants.ELP_ENTITY)
                        .concat(Contants.ELP_MAPPING_SEPARATOR)
                        .concat(entity.getElpTypeId)
                        , entity.getUpdateId)
                }

            }

            if (Option(elpModifyObj.getLinks).nonEmpty) {
                for (link <- elpModifyObj.getLinks.asScala) {
                    reMap.put(elpId.concat(Contants.ELP_MAPPING_SEPARATOR)
                        .concat(LinkContants.ELP_LINK)
                        .concat(Contants.ELP_MAPPING_SEPARATOR)
                        .concat(link.getElpTypeId)
                        , link.getUpdateId)
                }
            }
        }

        return reMap
    }

    def getELpMapByElpId(elpId: String, collection: String): ELpModifyMap = {
        val dbObj = loadMongoDao.getDBObjectByElp(elpId, collection)
        JSON.parseObject(dbObj.toString, classOf[ELpModifyMap])
    }

    def upInsert(elpMap: ELpModifyMap, collection: String): Unit = {
        val isExist = isExistByElp(elpMap.getElp, collection)
        if (isExist) {
            updateElpMap(elpMap, collection)
        } else {
            saveElpMap(elpMap, collection)
        }
    }

    def saveElpMap(elpMap: ELpModifyMap, collection: String): Unit = {
        loadMongoDao.save(elpMap, collection)
    }

    def saveElpMap(seqid: Long, elpMap: ELpModifyMap, collection: String): Unit = {
        elpMap.setSeqId(seqid)
        loadMongoDao.save(elpMap, collection)
    }

    /**
      * TODO
      */
    def updateSeqId(): Unit = {

    }

    def updateElpMap(elpMap: ELpModifyMap, collection: String): Unit = {
        loadMongoDao.update(elpMap, collection)
    }

    def getDataSchemaIdsByElpId(elpModelId: String, elpMap: ELpModifyMap): mutable.HashSet[String] = {
        val dataSchemaIdSet = new mutable.HashSet[String]()
        if (Option(elpMap.getEntities).nonEmpty) elpMap.getEntities.asScala.foreach(e => dataSchemaIdSet.add(e.getDataSchemaId))
        if (Option(elpMap.getLinks).nonEmpty) elpMap.getLinks.asScala.foreach(l => dataSchemaIdSet.add(l.getDataSchemaId))
        dataSchemaIdSet
    }

    def cacheMap(elpMap: ELpModifyMap): mutable.HashMap[String, mutable.HashSet[String]] = {
        val elpMapCache = new mutable.HashMap[String, mutable.HashSet[String]]()
        if (Option(elpMap.getEntities).nonEmpty) {
            val entitySet = new mutable.HashSet[String]()
            elpMap.getEntities.asScala.foreach(e => entitySet.add(e.getElpTypeId))
            elpMapCache.put(Contants.ENTITY, entitySet)
        }

        if (Option(elpMap.getLinks).nonEmpty) {
            val linkSet = new mutable.HashSet[String]()
            elpMap.getLinks.asScala.foreach(l => linkSet.add(l.getElpTypeId))
            elpMapCache.put(Contants.LINK, linkSet)
        }
        elpMapCache
    }

    def getElpDBMappings(dataSchemaIds: mutable.HashSet[String], elpMap: ELpModifyMap,
                         baseElpDBMappings: mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]): mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]] = {
        val effectDBMappings = new mutable.HashMap[String, ArrayBuffer[ElpModelDBMapping]]
        val elpMapCache = cacheMap(elpMap)
        dataSchemaIds.foreach(dsid => {
            val dBMappings = baseElpDBMappings.get(dsid)
            if (dBMappings.nonEmpty) {
                val arrayBuffer = new ArrayBuffer[ElpModelDBMapping]()
                dBMappings.get.foreach(mapping => {
                    if (mapping.getElpTypeDesc.toString.contains(Contants.ENTITY)) {
                        val entitySet = elpMapCache.get(Contants.ENTITY)
                        if (entitySet.nonEmpty) {
                            if (entitySet.get.contains(mapping.getElpType)) {
                                arrayBuffer += mapping
                            }
                        }
                    } else {
                        val linkSet = elpMapCache.get(Contants.LINK)
                        if (linkSet.nonEmpty) {
                            if (linkSet.get.contains(mapping.getElpType)) {
                                arrayBuffer += mapping
                            }
                        }
                    }
                })
                effectDBMappings.put(dsid, arrayBuffer)
            }
        })
        effectDBMappings
    }

}
