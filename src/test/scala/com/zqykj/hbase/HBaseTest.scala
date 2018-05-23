package com.zqykj.hbase

import java.text.SimpleDateFormat
import java.util.Date

import com.zqykj.streaming.common.JobConstants
import com.zqykj.streaming.util.HGlobalConn
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test

/**
  * Created by weifeng on 2018/3/3.
  */
class HBaseTest {

	val hbaseZKQuorum = "172.30.6.82"

	def getCollection(): Connection = {
		val conf = HGlobalConn.getConfiguration(hbaseZKQuorum)
		ConnectionFactory.createConnection(conf)
	}

	@Test
	def testStrDate(): Unit = {
		val hConnection = getCollection()
		val hTable = hConnection.getTable(TableName.valueOf("test"))
		val put = new Put(Bytes.toBytes("r10"))
		put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("datetime"), Bytes.toBytes("2018-01-20 12:12:12"))
		hTable.put(put)
		hTable.close()
		hConnection.close()
	}

	@Test
	def testLongDate(): Unit = {
		val hConnection = getCollection()
		val hTable = hConnection.getTable(TableName.valueOf("test"))
		val put = new Put(Bytes.toBytes("r13"))
		put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("datetime"), Bytes.toBytes("1517218516000"))
		hTable.put(put)
		hTable.close()
		hConnection.close()
	}

	@Test
	def testDate(): Unit = {
		val date = new SimpleDateFormat(JobConstants.FORMATTER_DATETIME).parse("2017-12-30 10:00:00")
		val hConnection = getCollection()
		val hTable = hConnection.getTable(TableName.valueOf("test"))
		val put = new Put(Bytes.toBytes("r13"))
		put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("datetime"), Bytes.toBytes("1517218516000"))
		hTable.put(put)
		hTable.close()
		hConnection.close()
	}

}
