package com.zqykj.batch.document.db

import com.mongodb.{DB, MongoClient, MongoCredential, ServerAddress}


/**
  * @author alfer
  * @date 1/4/18
  */
object MongoUtils {

    var options = Map[String, String](
        "host" -> "172.30.6.61",
        "database" -> "hyjj",
        "collection" -> "",
        "user" -> "zqykj",
        "password" -> "zqykj"
    )

    var mongoClient: MongoClient = null

    def apply(paramOptions: Map[String, String]) = {
        options = paramOptions
    }

    def getMongoClient(options: Map[String, String]): MongoClient = {
        val credential = MongoCredential.createCredential(options.get("user").getOrElse("zqykj"),
            options.get("database").getOrElse("hyjj"),
            options.get("password").getOrElse("zqykj").toCharArray)
        if (Option(mongoClient).isEmpty) {
            mongoClient = new MongoClient(new ServerAddress(options.get("host").getOrElse("test81")), java.util.Arrays.asList(credential))
        }
        mongoClient
    }

    def getDB(options: Map[String, String]): DB = {
        getMongoClient(options).getDB(options.get("database").getOrElse("hyjj"))
    }


}
