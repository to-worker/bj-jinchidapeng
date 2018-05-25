package com.zqykj.streaming.common

/**
  * @author feng.wei
  * @date 2018/5/22
  */
object JobPropertyConstant {

	val PATH_SEPERATOR_DEFAULT = "/"

	val WILDCARD = "*"

	val FILE_SUFFIX_EXE = ".txt"

	val TRANS_DEFAULT_FS_DEFAULT = "hdfs://172.30.6.34:8020"

	val TRANS_RES_ID_DEFAULT = "tel_net-2018-05-22"

	val TRANS_DATA_PATH_DEFAULT = "/outer/bjHive/tel_net/2018-05-22"

	val TRANS_DATASCHEMD_ID_DEFAULT = "1372bd05-b8e9-4767-95f2-021e2a0a72f7"

	val TRANS_TABLE_NAME_DEFAULT = "tel_net"

	val TRANS_DATE_DAY_DEFAULT = "2018-05-22"

	val ZOOKEEPER_SERVER_DEFAULT = "skynettest01,skynettest03:2181"

	val ZOOKEEPER_CH_ROOT_DEFAULT = "/solrv7testn"

	val SOLR_BATCH_SIZE_DEFAULT = 50000

	val SOLR_ENTITY_COLLECTION_NAME = "standard_model_entity_index"

	val SOLR_LINK_COLLECTION_NAME = "standard_model_relation_index"

	val MONGODB_HOST_DEFAULT = "172.30.6.34"


}
