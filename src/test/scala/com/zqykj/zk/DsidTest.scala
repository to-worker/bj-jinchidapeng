package com.zqykj.zk

import com.zqykj.streaming.util.DistIdUtils
import org.junit.Test

/**
  * @author alfer
  * @date 1/30/18
  */
class DsidTest {

    @Test
    def testGetDsid(): Unit = {
        DistIdUtils.apply("test81:2181")
        for (i <- 0 until 1000) {
            println(DistIdUtils.getSequenceId())
        }
    }

    @Test
    def testSortIds(): Unit = {
        val list = List(11111l, 33333, 12121)
        DistIdUtils.sortSeqIds(list).foreach(println(_))
    }

    @Test
    def testGetEffectIds(): Unit = {
        val list = List(11111l, 33333, 12121)
        val sortIds = DistIdUtils.sortSeqIds(list)
        println(s"sortIds: ${sortIds}")
        val effectIds = DistIdUtils.getEffectSeqIds(sortIds, 12100)
        println(s"effectIds: ${effectIds}")
    }


}
