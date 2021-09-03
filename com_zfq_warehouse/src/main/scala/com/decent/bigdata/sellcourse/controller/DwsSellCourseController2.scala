package com.decent.bigdata.sellcourse.controller


import com.decent.bigdata.sellcourse.service.DwsSellCourseService
import com.decent.bigdata.warehouse.utils.HiveUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")
//      .set("spark.sql.shuffle.partitions", "15")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtils.openDynamicPartition(sparkSession)
    HiveUtils.openCompression(sparkSession)
    DwsSellCourseService.importSellCourseDetail2(sparkSession, "20190722")
  }
}
