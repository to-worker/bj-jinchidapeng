package com.zqykj.streaming.db

import com.zqykj.streaming.util.HGlobalConn
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

/**
  * @author alfer
  * @date 12/15/17
  */
object HBaseOperator extends Logging {

    def getIncrementData(tableName: String, startTime: Long): Unit = {
        val connnection = HGlobalConn.getConnection("test81")
        val table = connnection.getTable(TableName.valueOf(tableName))
        val scan = new Scan()
        scan.setTimeRange(startTime, System.currentTimeMillis())
        var resultScanner: ResultScanner = null
        try {
            resultScanner = table.getScanner(scan)
            val iterator = resultScanner.iterator()
            var result: Result = null
            while (iterator.hasNext) {
                result = iterator.next()
                println(Bytes.toString(result.getRow))
            }
        } catch {
            case ex: NullPointerException => {
                logError(s"NullPointerException: ${ex.getMessage}", ex)
            }
        } finally {
            resultScanner.close()
            table.close()
            connnection.close()
        }

    }

    def main(args: Array[String]): Unit = {
        getIncrementData("preTest", 1513267200000l)
    }

}
