package com.decent.bigdata.warehouse.controller

import com.decent.bigdata.warehouse.service.AdsMemberService
import com.decent.bigdata.warehouse.utils.HiveUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdsMemberController {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("ads_member_controller")
			//.setMaster("local[*]")
		val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val ssc = sparkSession.sparkContext
		HiveUtils.openDynamicPartition(sparkSession) //开启动态分区
		AdsMemberService.queryDetailApi(sparkSession, "20190722")
		AdsMemberService.queryDetailSql(sparkSession, "20190722")
	}
}
