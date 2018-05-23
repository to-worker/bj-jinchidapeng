package com.zqykj.batch.transform.db

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zqykj.streaming.common.Contants.{EntityConstants, LinkContants}
import com.zqykj.streaming.util.HGlobalConn
import com.zqykj.tldw.util.HBaseUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author alfer
  * @date 1/23/18
  */
class HBasePutExecutor(@transient private val sc: SparkContext) extends Logging with Serializable {

    val hbaseZKQuorum = sc.getConf.get("spark.load.hbase.zk.quorum", "skynettest01")
    val hbaseContext = new HBaseContext(sc, HGlobalConn.getConfiguration(hbaseZKQuorum))

    def bulkPut(rdd: RDD[JSONObject], tableName: String, func: (JSONObject) => Put): Unit = {
        hbaseContext.bulkPut(rdd, TableName.valueOf(tableName), func)
    }

    def compactEntityPut(element: JSONObject): Put = {
        val rowKey = element.get(EntityConstants.HBASE_TABLE_ROWKEY)
        val put = new Put(Bytes.toBytes(HBaseUtils.md5Hash(rowKey.toString)))
        put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(EntityConstants.VERTEX_ID_FILED),
            Bytes.toBytes(element.get(EntityConstants.VERTEX_ID_FILED).toString))
        put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(EntityConstants.VERTEX_TYPE_FILED),
            Bytes.toBytes(element.get(EntityConstants.VERTEX_TYPE_FILED).toString))
        put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(EntityConstants.VERTEXT_RESID),
            Bytes.toBytes(element.get(EntityConstants.VERTEXT_RESID).toString))
        put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(EntityConstants.VERTEXT_OWNER),
            Bytes.toBytes(element.get(EntityConstants.VERTEXT_OWNER).toString))

        compactBody(put, element)
    }

    def compactLinkPut(element: JSONObject): Put = {
        val rowKey = element.get(EntityConstants.HBASE_TABLE_ROWKEY)
        val put = new Put(Bytes.toBytes(HBaseUtils.md5Hash(rowKey.toString)))
        put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_FROM_VERTEX_TYPE_FIELD)))
        put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_FROM_VERTEX_ID_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_FROM_VERTEX_ID_FIELD)))
        put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_TO_VERTEX_TYPE_FIELD)))
        put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_TO_VERTEX_ID_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_TO_VERTEX_ID_FIELD)))
        put.addColumn(LinkContants.HBASE_PROPERTY_LINK_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_DIRECTION_TYPE_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_DIRECTION_TYPE_FIELD)))

        put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_TYPE_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_TYPE_FIELD)))
        put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_ID_FIELD),
            Bytes.toBytes(element.getString(LinkContants.EDGE_ID_FIELD)))
        put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_RESID),
            Bytes.toBytes(element.getString(LinkContants.EDGE_RESID)))
        put.addColumn(LinkContants.HBASE_PROPERTY_FAMILY_BYTES,
            Bytes.toBytes(LinkContants.EDGE_OWNER),
            Bytes.toBytes(element.getString(LinkContants.EDGE_OWNER)))

        compactBody(put, element)
    }

    def compactBody(put: Put, element: JSONObject): Put = {
        val jsonBody = element.getJSONObject("body")
        val bodyIter = jsonBody.entrySet().iterator()
        while (bodyIter.hasNext) {
            val element = bodyIter.next()
            val key = element.getKey
            val value = element.getValue.toString
            // val kv = convertType(key, value)
            put.addColumn(EntityConstants.HBASE_PROPERTY_FAMILY_BYTES,
                Bytes.toBytes(key),
                Bytes.toBytes(JSON.parseObject(value).get("value").toString))
        }
        put
    }


}
