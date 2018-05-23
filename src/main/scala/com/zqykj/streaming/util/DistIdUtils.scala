package com.zqykj.streaming.util

import java.util.Properties

import com.zqykj.tools.distid.client.DistIdClient
import com.zqykj.tools.distid.exception.CoordinatorException
import com.zqykj.tools.distid.server.NocoorinatorClientHandle
import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * @author alfer
  * @date 1/26/18
  */
object DistIdUtils extends Logging with Serializable {

	var zkQum = "test82:2181"
	var properties: Properties = null

	def apply(zkQurom: String): Unit = {
		zkQum = zkQurom
		setProperty
	}

	def setProperty() = {
		val prop = new Properties()
		prop.setProperty(DistIdClient.KEY_COORDINATOR_TYPE, DistIdClient.CoodinatorType.NONE.toString)
		prop.setProperty(NocoorinatorClientHandle.KEY_ZK, zkQum)
		properties = prop
	}

	def getSequenceId(): Long = {
		var dsidTime: Long = System.currentTimeMillis()
		try {
			dsidTime = DistIdClient.getInstance(properties).getId
		} catch {
			case ex: CoordinatorException => {
				// close()
				logError(s"${ex}")
				//throw new Exception("get distributed id occur exception", ex)
			}
		} finally {
			// DistIdClient.getInstance(properties).free()
		}
		return dsidTime
	}

	def close(): Unit = {
		DistIdClient.getInstance(properties).free()
	}

	def compare(s1: Long, s2: Long): Boolean = {
		if (s1 > s2) true else false
	}

	def sortSeqIds(ids: List[Long]): List[Long] = {
		ids.sortBy(id => id)
	}

	/**
	  *
	  * @param seqids
	  * @param sequenceId
	  * @return
	  */
	def getEffectSeqIds(seqids: List[Long], sequenceId: Long): ArrayBuffer[Long] = {
		// get the effectids according sequenceId gt sequenceName in seqids
		val effectids = seqids.filter(id => DistIdUtils.compare(sequenceId, id))
		logInfo(s"seqids: ${seqids}, sequenceId: ${sequenceId}, effectids: ${effectids}")
		val seqidsArr = new ArrayBuffer[Long]()
		seqidsArr.appendAll(effectids)
		// get the sequenceName from sequenceId lt sequenceName
		if (!seqids.contains(sequenceId)) {
			val greaterIds = seqids.filterNot(id => DistIdUtils.compare(sequenceId, id)) //.head
			if (greaterIds.size > 0) {
				seqidsArr.append(greaterIds.head)
			}
		}
		logInfo(s"effect seqIds: ${seqidsArr}")
		seqidsArr
	}

}
