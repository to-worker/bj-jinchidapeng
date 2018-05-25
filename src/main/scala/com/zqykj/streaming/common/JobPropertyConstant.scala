package com.zqykj.streaming.common

/**
  * @author feng.wei
  */
object JobPropertyConstant {

	val PATH_SEPERATOR_DEFAULT = "/"

	val WILDCARD = "*"

	val FILE_SUFFIX_EXE = ".txt"

	val TRANS_DEFAULT_FS_DEFAULT = "hdfs://172.30.6.34:8020"

	val TRANS_RES_ID_DEFAULT = "relation-2018-05-22"

	val TRANS_DATA_PATH_DEFAULT = "/outer/bjHive/relation/2018-05-25"

	val TRANS_DATASCHEMD_ID_DEFAULT = "relation-dataSchemaId"

	val TRANS_TABLE_NAME_DEFAULT = "relation"

	val TRANS_DATE_DAY_DEFAULT = "2018-05-22"

	val ZOOKEEPER_SERVER_DEFAULT = "skynettest01,skynettest03:2181"

	val ZOOKEEPER_CH_ROOT_DEFAULT = "/solrv7testn"

	val SOLR_BATCH_SIZE_DEFAULT = 50000

	val SOLR_ENTITY_COLLECTION_NAME = "standard_model_entity_index"

	val SOLR_LINK_COLLECTION_NAME = "standard_model_relation_index"

	val MONGODB_HOST_DEFAULT = "172.30.6.34"


}
