package com.zqykj.streaming.dao

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.{BasicDBObject, DBObject}
import com.zqykj.batch.document.db.MongoUtils
import com.zqykj.batch.document.db.MongoUtils.getDB
import com.zqykj.streaming.metadata.ELpModifyMap
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
  * @author alfer
  * @date 1/27/18
  */
class LoadMongoDao(val options: Map[String, String]) {

    MongoUtils.apply(options)

    def isExistByElp(elpId: String, collection: String): Boolean = {
        val dbCollection = getDB(options).getCollection(collection)
        val query = new BasicDBObject()
        query.append("elp", elpId)
        val result = dbCollection.findOne(query)
        if (Option(result).isEmpty) {
            return false
        } else {
            return true
        }
    }

    def getOne(params: java.util.Map[String, Object], collection: String): ELpModifyMap = {
        val dbCollection = getDB(options).getCollection(collection)
        val query = new BasicDBObject()
        if (Option(params).nonEmpty) {
            val iter: java.util.Iterator[String] = params.keySet().iterator()
            while (iter.hasNext) {
                val ele = iter.next()
                query.append(ele, params.get(ele))
            }
        }
        val dbObj = dbCollection.findOne(query)
        if (Option(dbObj).isEmpty) {
            null
        } else {
            JSON.parseObject(dbObj.toString, classOf[ELpModifyMap])
        }
    }

    def getDBObjectByElp(elpId: String, collection: String): DBObject = {
        val dBCollection = getDB(options).getCollection(collection)
        val dBObject = new BasicDBObject()
        dBObject.append("elp", elpId)
        dBCollection.findOne(dBObject)
    }

    def save(logMap: java.util.Map[String, _], collection: String): Unit = {
        val db = getDB(options)
        val dbCollection = db.getCollection(collection)
        val dBObject = new BasicDBObject()
        dBObject.putAll(logMap)
        dbCollection.save(dBObject)
    }

    def save(elpMap: ELpModifyMap, collection: String): Unit = {
        val db = getDB(options)
        val dbCollection = db.getCollection(collection)
        dbCollection.insert(getDBObject(elpMap))
    }

    private def getDBObject(obj: Object): DBObject = {
        com.mongodb.util.JSON.parse(obj.toString).asInstanceOf[DBObject]
    }

    def update(elpMap: ELpModifyMap, collection: String): Unit = {
        val db = getDB(options)
        val dBCollection = db.getCollection(collection)
        val oriDBObject = getDBObjectByElp(elpMap.getElp, collection)
        val newDBObject = getDBObject(elpMap)
        dBCollection.update(oriDBObject, newDBObject)
    }

    private def serialiableObject(obj: Object): BasicDBObject = {
        val dbo = new BasicDBObject()
        val outputStream = new ByteArrayOutputStream()
        val out = new ObjectOutputStream(outputStream)
        out.writeObject(obj)
        dbo.put("JavaObject", outputStream.toByteArray())
        out.close()
        outputStream.close()
        dbo
    }

}
