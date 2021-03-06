package com.atguigu.warehouse.controller

import com.atguigu.warehouse.service.DwsMemberService
import com.atguigu.warehouse.utils.HiveUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsMemberController {
	def main(args: Array[String]): Unit = {
		System.setProperty("HADOOP_USER_NAME", "atguigu")
		var sparkConf = new SparkConf().setMaster("local[*]").setAppName("dws_member_import").set("spark.sql.shuffle.partitions", "60")
		val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val ssc = session.sparkContext
		HiveUtils.openDynamicPartition(session) //动态分区
		HiveUtils.openCompression(session)//开启压缩
		DwsMemberService.importMember(session, "20190722") //根据用户信息聚合用户表数据
		DwsMemberService.importMemberUseApi(session, "20190722")
	}
}
