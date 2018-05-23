package com.zqykj.streaming.util

import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.zqykj.hyjj.entity.elp.{PropertyBag, PropertyType}
import com.zqykj.streaming.common.Contants.PropertyTypeConstants
import com.zqykj.streaming.common.JobConstants
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

import scala.collection.JavaConverters._

/**
  * Created by alfer on 9/7/17.
  */
object TypeConvertUtils extends Logging with Serializable {

	def getBytes(key: String, value: Any, element: PropertyBag): Array[Byte] = {
		if (null == value) return null
		if (value.isInstanceOf[Integer]) return Bytes.toBytes(value.asInstanceOf[Integer])
		if (value.isInstanceOf[Boolean]) return Bytes.toBytes(value.asInstanceOf[Boolean])
		if (value.isInstanceOf[Double]) return Bytes.toBytes(value.asInstanceOf[Double])
		if (value.isInstanceOf[Float]) return Bytes.toBytes(value.asInstanceOf[Float])
		if (value.isInstanceOf[Array[Byte]]) return value.asInstanceOf[Array[Byte]]
		if (value.isInstanceOf[Long]) { // 12:30
			if (element.getPropertyByUUID(key).getType == PropertyType.time) {
				val sdf = new SimpleDateFormat(JobConstants.FORMATTER_TIME)
				return Bytes.toBytes(sdf.format(new Date(value.asInstanceOf[Long])))
			}
			return Bytes.toBytes(value.asInstanceOf[Long])
		}
		if (value.isInstanceOf[String]) {
			if (element.getPropertyByUUID(key).getType == PropertyType.number) return Bytes.toBytes(new lang.Double(value.asInstanceOf[String]))
			if (element.getPropertyByUUID(key).getType == PropertyType.integer) return Bytes.toBytes(new Integer(value.asInstanceOf[String]))
			return Bytes.toBytes(value.asInstanceOf[String])
		}
		Bytes.toBytes(value.asInstanceOf[String])
	}

	def getTypeConvert(fType: String, value: String): Object = {
		if ("".equals(value)) return null
		// 日期时间为字符串类型
		// 日期时间为日期类型 =》 Long
		// number 类型
		try {
			if ("date".equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_LONG)) {
				val sdf = new SimpleDateFormat(JobConstants.FORMATTER_DATE)
				return sdf.parse(sdf.format(new Date(value.toLong)))
			} else if ("date".equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_DATE)) {
				return new SimpleDateFormat(JobConstants.FORMATTER_DATE).parse(value)
			} else if ("date".equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_DATETIME)) {
				return new SimpleDateFormat(JobConstants.FORMATTER_DATETIME).parse(value)
			} else if ("datetime".equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_LONG)) {
				val sdf = new SimpleDateFormat(JobConstants.FORMATTER_DATETIME)
				return sdf.parse(sdf.format(new Date(value.toLong)))
			} else if ("datetime".equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_DATETIME)) {
				return new SimpleDateFormat(JobConstants.FORMATTER_DATETIME).parse(value)
			} else if ("datetime".equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_DATE)) {
				return new SimpleDateFormat(JobConstants.FORMATTER_DATE).parse(value)
			} else if ("number".equals(fType) && !value.matches(JobConstants.REGEX_NUMBER_FORMAATER)) {
				return int2Integer(-1)
			} else {
				return value
			}
		} catch {
			case ex: NumberFormatException => {
				logError(s"convert type has exception: ${ex.getStackTraceString}")
			}
		}
		value
	}


	/**
	  * if value is empty string, return null.
	  * if value matches long type, parse to Date type using SimpleDateFormat.
	  * if value matches other type, parse to Date tpye using FastDateFormat.
	  *
	  * @param fType
	  * @param value
	  * @param datePatterns
	  * @return
	  */
	def getTypeConvert(fType: String, value: String, datePatterns: java.util.List[String]): Object = {
		if ("".equals(value)) return null
		var msg: String = ""
		try {
			if (PropertyTypeConstants.date.equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_LONG)) {
				val sdf = new SimpleDateFormat(JobConstants.FORMATTER_DATE)
				return sdf.parse(sdf.format(new Date(value.toLong)))
			}

			if (PropertyTypeConstants.datetime.equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_LONG)) {
				val sdf = new SimpleDateFormat(JobConstants.FORMATTER_DATETIME)
				return sdf.parse(sdf.format(new Date(value.toLong)))
			}

			if (PropertyTypeConstants.time.equals(fType) && value.matches(JobConstants.REGEX_DATE_FORMATTER_LONG)) {
				val sdf = new SimpleDateFormat(JobConstants.FORMATTER_TIME)
				return sdf.parse(sdf.format(new Date(value.toLong)))
			}

			if (PropertyTypeConstants.date.equals(fType) && (value.matches(JobConstants.REGEX_DATE_FORMATTER_DATE)
				|| value.matches(JobConstants.REGEX_DATE_FORMATTER_DATETIME))) {
				return new SimpleDateFormat(JobConstants.FORMATTER_DATE).parse(value)
			}

			if (PropertyTypeConstants.datetime.equals(fType)) {
				if (value.matches(JobConstants.REGEX_DATE_FORMATTER_DATETIME)) {
					return new SimpleDateFormat(JobConstants.FORMATTER_DATETIME).parse(value)
				} else if (value.matches(JobConstants.REGEX_DATE_FORMATTER_DATE)) {
					return new SimpleDateFormat(JobConstants.FORMATTER_DATETIME).parse(value.concat(" 00:00:00"))
				}
			}

			if (PropertyTypeConstants.time.equals(fType)) {
				if (value.matches(JobConstants.REGEX_DATE_FORMATTER_TIME)) {
					return new SimpleDateFormat(JobConstants.FORMATTER_TIME).parse(value)
				} else if (value.matches(JobConstants.REGEX_DATE_FORMATTER_DATETIME)) {
					return new SimpleDateFormat(JobConstants.FORMATTER_DATE_DEFAULT)
				}
			}

		} catch {
			case ex: NumberFormatException => {
				logError(s"convert type has exception: ${ex.getStackTraceString}")
				msg = ex.getStackTraceString
			}
			case ex: Exception => {
				logError(s"convert ${value} to ${fType} type has exception: ${ex.getStackTraceString}")
				msg = ex.getStackTraceString
			}
		}
		// var e: Exception = null
		for (pattern <- datePatterns.asScala) {
			try {
				val format = FastDateFormat.getInstance(pattern, TimeZone.getTimeZone("GMT+8"))
				val date = format.parse(value)
				if (value.length == pattern.length) {
					return date
				}
			} catch {
				case ex: Exception => {
					// e = ex
					msg = ex.getStackTraceString
				}
			}
		}
		// throw new TransformerException("没有找到匹配的pattern", e)
		if (!"".equals(msg)) {
			logError(s"convert ${value} to  ${fType} type has exception: ${msg}")
		}
		return null
	}


}
