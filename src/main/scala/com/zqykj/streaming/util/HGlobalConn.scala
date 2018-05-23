package com.zqykj.streaming.util

import com.zqykj.streaming.common.JobConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * Created by alfer on 9/7/17.
  */
object HGlobalConn {

    def getConfiguration(zk: String): Configuration = {
        val conf = HBaseConfiguration.create()
        conf.set(JobConstants.HBASE_ZOOKEEPER_QUORUM, zk)
        conf.setInt(JobConstants.HBASE_ZOOKEEPER_CLIENTPORT, 2181)
        conf.set(JobConstants.HBASE_ZOOKEEPER_ZNODE_PARENT, JobConstants.HBASE_ZOOKEEPER_ZNODE_PARENT_DEFAULT)
        conf
    }

    def getConnection(zk: String): Connection = {
        val conf = HBaseConfiguration.create()
        conf.set(JobConstants.HBASE_ZOOKEEPER_QUORUM, zk)
        conf.setInt(JobConstants.HBASE_ZOOKEEPER_CLIENTPORT, 2181)
        conf.set(JobConstants.HBASE_ZOOKEEPER_ZNODE_PARENT, JobConstants.HBASE_ZOOKEEPER_ZNODE_PARENT_DEFAULT)
        ConnectionFactory.createConnection(conf)
    }

    def apply(zkQuorum: String): Connection = {
        getConnection(zkQuorum)
    }

}
