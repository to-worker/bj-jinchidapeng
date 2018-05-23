package com.zqykj.batch.document.db

import com.zqykj.batch.document.job.DocDataCollectJob
import com.zqykj.streaming.common.Contants
import com.zqykj.streaming.util.HGlobalConn
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author alfer at 12/22/17
  */
class HBaseExecutor(@transient private val sc: SparkContext,
                    val residSetBroadcast: Broadcast[mutable.HashSet[String]]) extends Logging with Serializable {

	val hbaseZKQuorum = sc.getConf.get("spark.batch.hbase.zk.quorum", "zqykjdev14")
	private val hBaseContext = new HBaseContext(sc, HGlobalConn.getConfiguration(hbaseZKQuorum))
	val residSet = residSetBroadcast.value

	def scan(tableName: String, scan: Scan): RDD[Result] = {
		hBaseContext.hbaseRDD(TableName.valueOf(tableName), scan).map(m => m._2)
	}

	def buildScan(family: Array[Byte], id: String, columns: ArrayBuffer[String]): Scan = {
		val scan = new Scan()
		try {
			scan.addColumn(family, Bytes.toBytes(id))
			for (column <- columns) {
				scan.addColumn(family, Bytes.toBytes(column))
			}
		}
		scan
	}

	def buildScan(family: Array[Byte], id: String, columns: ArrayBuffer[String], minTimeStamp: Long, maxTimeStamp: Long): Scan = {
		val scan = new Scan()
		try {
			scan.setTimeRange(minTimeStamp, maxTimeStamp)
			scan.addColumn(family, Bytes.toBytes(id))
			scan.addColumn(family, Contants.HBASE_RESID_BYTES)
			for (column <- columns) {
				scan.addColumn(family, Bytes.toBytes(column))
			}
		}
		scan
	}

	def isFilterByProperty(res: Result, family: Array[Byte], filterProperty: Array[Byte]): Boolean = {
		val cellOpt = Option(res.getColumnLatestCell(family, filterProperty))
		var isThrowAway = false
		if (cellOpt.nonEmpty) {
			val cell = cellOpt.get
			val resid = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
			if (!residSet.contains(resid)) {
				isThrowAway = true
			}
		}
		isThrowAway
	}

	def computeRDD(rdd: RDD[Result], family: Array[Byte], key: String, properties: ArrayBuffer[(String, String)]): RDD[(String, ArrayBuffer[(String, String)])] = {
		val filteredRDD = rdd.filter(res => {
			isFilterByProperty(res, family, Contants.HBASE_RESID_BYTES)
		})
		val r = filteredRDD.mapPartitions(mp => {
			val keyBufferList = mp.map(res => {
				val listBuffer = ArrayBuffer[(String, String)]()
				val keyCellOpt = Option(res.getColumnLatestCell(family, Bytes.toBytes(key)))
				// println(s"key: ${key}, keyCell: ${keyCell}")
				var kValue: String = ""
				if (keyCellOpt.nonEmpty) {
					val keyCell = keyCellOpt.get
					kValue = Bytes.toString(keyCell.getValueArray, keyCell.getValueOffset, keyCell.getValueLength)
					for (p <- properties) {
						val pCellOpt = Option(res.getColumnLatestCell(family, Bytes.toBytes(p._1)))
						if (pCellOpt.nonEmpty) {
							val pCell = pCellOpt.get
							val pairValue = Bytes.toString(pCell.getValueArray, pCell.getValueOffset, pCell.getValueLength)
							val pv = (p._2, pairValue)
							listBuffer += pv
						}
					}
				}
				(kValue, listBuffer)
			})
			keyBufferList
		})
		r
	}

	def computeTargetRDDs(elpModel: String, elpType: String, family: Array[Byte],
	                      targetEntityRules: ArrayBuffer[(String, String, String, String)],
	                      minTimeStamp: Long, maxTimeStamp: Long): ArrayBuffer[RDD[(String, ArrayBuffer[(String, String)])]] = {
		val arrayBuffer = ArrayBuffer[RDD[(String, ArrayBuffer[(String, String)])]]()
		targetEntityRules.foreach(tRule => {
			val targetEntityTableName = elpModel
				.concat(Contants.HBASE_TABLE_SEPERATOR)
				.concat(elpType)
				.concat(Contants.HBASE_TABLE_SEPERATOR)
				.concat(tRule._2)
			val targetEntityRDD = scan(targetEntityTableName, buildScan(family, tRule._3, ArrayBuffer(tRule._4), minTimeStamp, maxTimeStamp))
			val targetEntityPropsRDD = computeRDD(targetEntityRDD, family, tRule._3, ArrayBuffer((tRule._4, tRule._1)))
			val cleanRDD = targetEntityPropsRDD.filter(f => !"".equals(f._1))
			arrayBuffer += cleanRDD
		})
		arrayBuffer
	}

	/**
	  *
	  * @param rdd      结果数据集
	  * @param family
	  * @param key      关联标识
	  * @param property (sPLevel1Name, dPLevel1Name)
	  * @return RDD[(sPIdLevel1Value, (dPLevel1Name, dPLevel1Value))]
	  */
	def computeRDDByProp(rdd: RDD[Result], family: Array[Byte], key: String, property: (String, String)): RDD[(String, (String, String))] = {
		val filteredRDD = rdd.filter(res => {
			isFilterByProperty(res, family, Contants.HBASE_RESID_BYTES)
		})
		val r = filteredRDD.mapPartitions(mp => {
			val keyBufferList = mp.map(res => {
				val listBuffer = ArrayBuffer[(String, String)]()
				val keyCellOpt = Option(res.getColumnLatestCell(family, Bytes.toBytes(key)))
				// println(s"key: ${key}, keyCell: ${keyCell}")
				var kValue: String = ""
				var pairValue: String = ""
				if (keyCellOpt.nonEmpty) {
					val keyCell = keyCellOpt.get
					kValue = Bytes.toString(keyCell.getValueArray, keyCell.getValueOffset, keyCell.getValueLength)
					val pCellOpt = Option(res.getColumnLatestCell(family, Bytes.toBytes(property._1)))
					if (pCellOpt.nonEmpty) {
						val pCell = pCellOpt.get
						pairValue = Bytes.toString(pCell.getValueArray, pCell.getValueOffset, pCell.getValueLength)
					}
				}
				(kValue, (property._2, pairValue))
			})
			keyBufferList
		})
		r
	}

	def foeachPrint(lable: String, rdd: RDD[(String, ArrayBuffer[(String, String)])]): Unit = {
		rdd.foreach(f => println(s"${lable}===> ${f}"))
	}


	def computeExtensionRDDs(elpModel: String, elpType: String, family: Array[Byte],
	                         targetEntityRules: ArrayBuffer[((String, String, String, String), ArrayBuffer[(String, String, String, String)])],
	                         minTimeStamp: Long, maxTimeStamp: Long): ArrayBuffer[RDD[(String, ArrayBuffer[(String, String)])]] = {
		val arrayBuffer = ArrayBuffer[RDD[(String, ArrayBuffer[(String, String)])]]()
		targetEntityRules.foreach(tRule => {
			val targetEntityTableName = elpModel
				.concat(Contants.HBASE_TABLE_SEPERATOR)
				.concat(elpType)
				.concat(Contants.HBASE_TABLE_SEPERATOR)
				.concat(tRule._1._2)
			val targetEntityRDD = scan(targetEntityTableName, buildScan(family, tRule._1._3, ArrayBuffer(tRule._1._4), minTimeStamp, maxTimeStamp))
			val targetEntityPropsRDD = computeRDDByProp(targetEntityRDD, family, tRule._1._3, (tRule._1._4, tRule._1._1))
			// level1 ==> RDD[(key,(dPName,dPValue))]
			val baseRDD = targetEntityPropsRDD.filter(f => !"".equals(f._1))
			// get level2 rdd ==> ArrayBuffer[RDD[(level2Key, ArrayBuffer[(dPName, dPValue)])]]
			val level2ExtensionRDDs = computeTargetRDDs(elpModel, Contants.HBASE_TABLE_NAME_ENTITY, family, tRule._2, minTimeStamp, maxTimeStamp)
			// ==> RDD[(level2Key, ArrayBuffer[(dPName, dPValue)])]
			val combinedRDD = DocDataCollectJob.unionArrayBuf(level2ExtensionRDDs)
			// ==> RDD[(dPValue, ArrayBuffer(key-value, dPName))]
			val reBaseRDD = baseRDD.map(m => {
				(m._2._2, ArrayBuffer((Contants.START_WITH_KEY.concat(m._1), m._2._1)))
			})
			val reCombinedRDD = reBaseRDD.leftOuterJoin(combinedRDD).map(m => {
				val tmpBuffer = m._2._1
				val optBuffer: Option[ArrayBuffer[(String, String)]] = m._2._2
				val reBuffer = if (m._2._2.nonEmpty) {
					(m._1, tmpBuffer.union(optBuffer.get))
				} else {
					(m._1, tmpBuffer)
				}
				reBuffer
			})
			val reducedRDD = reCombinedRDD.reduceByKey((a, b) => a.union(b))
			val keyLength = Contants.START_WITH_KEY.length
			val reComb = reducedRDD.map(m => {
				val l2KeyValue = m._1
				val eleArrayBuf = ArrayBuffer[(String, String)]()
				var rKey = ""
				m._2.foreach(f => {
					val tup = f._1
					if (tup.startsWith(Contants.START_WITH_KEY)) {
						rKey = tup.substring(keyLength)
						val kt = (f._2, l2KeyValue)
						eleArrayBuf += kt
					} else {
						eleArrayBuf += f
					}
				})
				(rKey, eleArrayBuf)
			})
			arrayBuffer += reComb
		})
		arrayBuffer
	}

}
