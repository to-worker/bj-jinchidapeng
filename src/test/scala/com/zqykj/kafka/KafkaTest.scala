package com.zqykj.kafka

import com.alibaba.fastjson.JSONObject
import kafka.utils.ZkUtils
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{Assert, Test}

/**
  * @author alfer
  * @date 1/18/18
  */
class KafkaTest extends Assert {

    /**
      * kafka.utils.ZkUtils 根据zkUrl获取broker地址
      */
    @Test
    def getTopics(): Unit = {
        val zkUtils = ZkUtils.apply("bigdatacluster01,bigdatacluster02,bigdatacluster03", 10000, 10000, false)
        val allBrokersInCluster = zkUtils.getAllBrokersInCluster
        for (broker <- allBrokersInCluster) {
            val brokerEndPoint = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)
            println(brokerEndPoint.connectionString())
        }
    }

    @Test
    def getJsonObj(): Unit = {
        val jsonObj = new JSONObject()
        jsonObj.put("k1", "v1")
        jsonObj.put("k2", "v2")
        println(jsonObj.get("k1"))
        println(Option(jsonObj.get("kk")).getOrElse(""))


    }

}
