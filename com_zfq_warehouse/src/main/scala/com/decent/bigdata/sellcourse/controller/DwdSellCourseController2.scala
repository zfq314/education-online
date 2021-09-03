package com.decent.bigdata.sellcourse.controller


import com.decent.bigdata.sellcourse.service.DwdSellCourseService
import com.decent.bigdata.warehouse.utils.HiveUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_sellcourse_import")
    //.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    //设置分桶相关参数
    //    sparkSession.sql("set hive.enforce.bucketing=false")
    //    sparkSession.sql("set hive.enforce.sorting=false")
    HiveUtils.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtils.openCompression(sparkSession) //开启压缩
    DwdSellCourseService.importCoursePay2(ssc, sparkSession)
    DwdSellCourseService.importCourseShoppingCart2(ssc, sparkSession)
  }
}
