package com.zqykj.streaming.common

import java.io.File

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by alfer on 9/2/17.
  */
object Contants {

	val SEPERATOR = File.separator
	val FULL_SEQUENCEID = -1

	val START_WITH_KEY = "key"

	val ELP_MODELS = "ELPModel"
	val ELP_MODEL_DB_MAPPING = "ElpModelDBMapping"
	val ORI_DATA_SCHEMAS = "OriginalDataSchema"
	val DATA_SCHEMAS = "DataSchema"
	val ELP_TOPIC_MAPPING = "ElpModelTopicMapping"

	val ELP_STREAM_LOAD_ENTITIES_LINKS = "ELPMStreamLoadEntitiesAndLinks"
	val ELP_TRANS_ENTITIS_LINKS = "ELPMTransEntitiesAndLinks"
	val ELP_TRANS_TASK_HISTORY = "ELPMTransTaskHistory"
	val SEQUENCEID_DATASCHEMAID_RESID = "@"
	val ID_ELP_TYPE_SEPERATOR = "@"

	val ORI_DATA_TYPEID = "typeId"
	val ORI_DATA_BODY = "body"

	val ELP_MAPPING_SEPARATOR = "_"
	val ID_SPACE_MARK = "~`#"
	val HBASE_TABLE_NAME_SEPARATOR = "_"
	val DSID_DS_TABLENAME = "#"

	val GLOBAL_GRAPH_ENTITY = "global_${modelId}_entity"
	val GLOBAL_GRAPH_LINK = "global_${modelId}_relation"
	val GLOBAL_GRAPH_ENTITY_INDEX = "global_${modelId}_entity_index"
	val GLOBAL_GRAPH_LINK_INDEX = "global_${modelId}_relation_index"

	val SOLR_COLLECTION_SUFFIX = "index"
	val SOLR_COLLECTION_NAME_SEPARATOR = "_"

	val ENTITY = "Entity"
	val LINK = "Link"
	val HBASE_TABLE_NAME_ENTITY = "entity"
	val HBASE_TABLE_NAME_RELATION = "relation"
	val HBASE_TABLE_SEPERATOR = "_"
	val FAMILY_PROPERTY_BYTES = Bytes.toBytes("property")
	val HBASE_RESID = "resid"
	val HBASE_RESID_BYTES = Bytes.toBytes("resid")

	/**
	  * integer,
	  * number,
	  * text,
	  * date,
	  * string,
	  * datetime,
	  * time,
	  * bool
	  */
	object PropertyTypeConstants {
		val bool = "bool"
		val date = "date"
		val datetime = "datetime"
		val time = "time"
		val integer = "integer"
		val number = "number"
		val text = "text"
		val string = "string"
	}

	object DocEntityConstants {
		val DOC_ENTITY_ID = "id"
		val DOC_ENTITY_DOC_TYPE = "doc_type"
		val DOC_ENTITY_DOC_KEY = "doc_keyvalue"
		val DOC_ENTITY_ID_KEY_SEPERATOR = "@"
		val DOC_ENTITY_PEOPLE_SHOW_TAG = "重点人员"
	}

	object EntityConstants {
		val ELP_ENTITY = "entity"
		val HBASE_TABLE_ROWKEY = "rowkey"
		val VERTEX_LABEL_FILED = "label"
		val VERTEX_TYPE_FILED = "entity_type"
		val VERTEX_ID_FILED = "entity_id"

		val HBASE_TABLE_ID = "id"
		val VERTEXT_DSID = "dsId"
		val VERTEXT_RESID = "resid"
		val VERTEXT_RESID_UPPER = "resId"
		val VERTEXT_OWNER = "owner"
		val HBASE_PROPERTY_FAMILY = "property"
		lazy val HBASE_PROPERTY_FAMILY_BYTES = Bytes.toBytes(HBASE_PROPERTY_FAMILY)
		val SOLR_COLLECTION_SUFFIX = "entity_index"
		val SOLR_COLLECTION_SEPERATOR = "entity_"

	}

	object LinkContants {
		val ELP_LINK = "relation"
		val LINK_SOURCE = "source"
		val LINK_TARGET = "target"

		val HBASE_TABLE_ROWKEY = "rowkey"
		val HBASE_TABLE_ID = "id"
		val EDGE_LABEL_FIELD = "label"
		val EDGE_TYPE_FIELD = "relation_type"
		val EDGE_ID_FIELD = "relation_id"
		val EDGE_FROM_VERTEX_TYPE_FIELD = "from_entity_type"
		val EDGE_FROM_VERTEX_ID_FIELD = "from_entity_id"
		val EDGE_TO_VERTEX_TYPE_FIELD = "to_entity_type"
		val EDGE_TO_VERTEX_ID_FIELD = "to_entity_id"
		val EDGE_DIRECTION_TYPE_FIELD = "direction_type"

		val SOLR_COLLECTION_SEPERATOR = "relation_"
		val SOLR_COLLECTION_SUFFIX = "relation_index"
		val EDGE_DSID = "dsId"
		val EDGE_RESID = "resid"
		val EDGE_RESID_UPPER = "resId"
		val EDGE_OWNER = "owner"
		val HBASE_PROPERTY_LINK_FAMILY = "link"
		lazy val HBASE_PROPERTY_LINK_FAMILY_BYTES = Bytes.toBytes(HBASE_PROPERTY_LINK_FAMILY)
		val HBASE_PROPERTY_FAMILY = "property"
		lazy val HBASE_PROPERTY_FAMILY_BYTES = Bytes.toBytes(HBASE_PROPERTY_FAMILY)

		/**
		  * 无向
		  */
		val DIRECTION_UNDIRECTED = "0"

		/**
		  * 单向
		  */
		val DIRECTION_UNIDIRECTIONAL = "1"

		/**
		  * 双向
		  */
		val DIRECTION_BIDIRECTIONAL = "2"
	}

	object Global {
		val GLOBAL = "global"
	}

	object RMDB {
		val MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver"
	}

}
