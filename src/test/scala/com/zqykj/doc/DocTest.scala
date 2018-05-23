package com.zqykj.doc

import com.zqykj.batch.document.job.DocDataCollectJob
import org.junit.Test

/**
  * Created by weifeng on 2018/5/5.
  */
class DocTest {

	@Test
	def testTotalCount(): Unit = {
		val totalCount = 300001413l
		val avrgSize = 432
		val totalSize = DocDataCollectJob.getTotalSize(totalCount, avrgSize)
		println(s"totalSize: ${totalSize}")

		println(s"(totalCount * avrgSize): ${(totalCount * avrgSize)}")
		println(s"(totalCount * avrgSize) / 1024: ${(totalCount * avrgSize) / 1024}")
		println(s"(totalCount * avrgSize) / 1024 / 1204: ${(300001413 * 432) / 1024 / 1204}")

	}

}
