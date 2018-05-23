package com.zqykj.exception

import java.security.MessageDigest

import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.junit.Test

/**
  * @author alfer
  * @date 2/1/18
  */
class ExceptionTest {

	@Test
	def testNullPointerException(): Unit = {
		try {
			val set1 = null
			val set2 = Set(1, 2)
			println(set2)
			set1.toString
		} catch {
			case ex: NullPointerException => println("null pointer exception")
		}
		val set3 = List(4, 5)
		println(set3)
	}


	@Test
	def testMD5(): Unit = {
		// 470dd4a14889b9a408707a8526fe90b8
		// 98eda3fbfb9ece24d00ed32b1cca382b
		val md = MD5Hash.getMD5AsHex("you are best..".getBytes())
		println(md)
	}

}
