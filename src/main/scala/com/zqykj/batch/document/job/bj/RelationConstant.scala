package com.zqykj.batch.document.job.bj

import scala.collection.mutable

/**
  * Created by weifeng on 2018/5/25.
  */
object RelationConstant {

	lazy val RELATION_ID_TYPES = {
		val set = new mutable.HashSet[String]
		set.add("mail")
		set.add("imsi")
		set.add("imei")
		set.add("mac")
		set.add("phone")
		set
	}

}
