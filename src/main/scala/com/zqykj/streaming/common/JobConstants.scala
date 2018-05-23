package com.zqykj.streaming.common

/**
  * @author alfer
  * @date 1/29/18
  */
object JobConstants {


    val avroExt = ".avro"
    val wildCardAvroExt = "*.avro"

    val finalStatus = "finalStatus"
    val finalStatus_s = "SUCCEEDED"
    val finalStatus_f = "FAILED"
    val finalErrorMsg = "errorMsg"

    val HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    val HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"
    val HBASE_ZOOKEEPER_CLIENTPORT_DEFAULT = 2181
    val HBASE_ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent"
    val HBASE_ZOOKEEPER_ZNODE_PARENT_DEFAULT = "/hbase"

    val FORMATTER_DATE = "yyyy-MM-dd"
    val FORMATTER_DATETIME = "yyyy-MM-dd HH:mm:ss"
    val FORMATTER_TIME = "HH:mm:ss"
    val REGEX_DATE_FORMATTER_LONG = "^[0-9]+$"
    val REGEX_DATE_FORMATTER_DATE = "^[0-9]{4}(-[0-9]{1,2}){2}$"
    val REGEX_DATE_FORMATTER_DATETIME = "^[0-9]{4}(-[0-9]{1,2}){2}\\s[0-9]{1,2}(:[0-9]{1,2}){2}$"
    val REGEX_DATE_FORMATTER_TIME = "^[0-9]{1,2}(:[0-9]{1,2}){2}$"
    val REGEX_NUMBER_FORMAATER = "^\\d*.?\\d*$"

    val FORMATTER_TIME_DEFAULT = "yyyy-MM-dd 00:00:00"
    val FORMATTER_DATE_DEFAULT = "1970-01-01 HH:mm:ss"

}
