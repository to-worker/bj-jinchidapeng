package com.zqykj.json

import com.alibaba.fastjson.{JSON, JSONObject}
import org.junit.Test

/**
  * Created by weifeng on 2018/5/24.
  */
class JsonTest {

	@Test
	def testParseArray(): Unit = {
		val str = "[{\"id_type\":\"phone\",\"id\":\"1234\"},{\"id_type\":\"mail\",\"id\":\"1234@sina.com\"}]"
		val jsonArray = JSON.parseArray(str)
		val jsonArrSize = jsonArray.size()
		for (i <- 0 until(jsonArrSize - 1)){
			println(s"json: ${jsonArray.getJSONObject(i)}")
		}
		val jsonObj = jsonArray.getJSONObject(0)
		println(s"jsonArray:${jsonArray}")
		println(s"jsonObject:${jsonObj}, id:${jsonObj.get("id")}, id_type: ${jsonObj.get("id_type")}")
	}

}
