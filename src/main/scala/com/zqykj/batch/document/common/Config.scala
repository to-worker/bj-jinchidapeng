package com.zqykj.batch.document.common

import com.zqykj.streaming.common.JobConstants
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * @author alfer
  * @date 12/22/17
  */
object Config {

    val zkQuorum = "172.30.6.81"
    val zkPort = 2181
    val solrZkQuorum = "test81:2181/solr"
    val docCollection = "doc_card_index_info"
    val solrBatch = 1

    lazy val configuration = {
        val conf = HBaseConfiguration.create()
        conf.set(JobConstants.HBASE_ZOOKEEPER_QUORUM, zkQuorum)
        conf.setInt(JobConstants.HBASE_ZOOKEEPER_CLIENTPORT, zkPort)
        conf.set(JobConstants.HBASE_ZOOKEEPER_ZNODE_PARENT, JobConstants.HBASE_ZOOKEEPER_ZNODE_PARENT_DEFAULT)
        conf
    }

}
