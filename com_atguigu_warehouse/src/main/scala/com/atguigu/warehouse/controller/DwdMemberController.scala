package com.atguigu.warehouse.controller

import com.atguigu.warehouse.service.EtlDataService
import com.atguigu.warehouse.utils.HiveUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdMemberController {
	def main(args: Array[String]): Unit = {
		var conf = new SparkConf().setMaster("local[*]").setAppName("dwd_member_controller")
		var sparkSession = SparkSession
			.builder()
			.config(conf)
			.enableHiveSupport()
			.getOrCreate()
		val ssc: SparkContext = sparkSession.sparkContext
		//开启动态分区
		HiveUtils.openDynamicPartition(sparkSession)
		//开启压缩
		HiveUtils.openCompression(sparkSession)
		//使用snappy压缩
		HiveUtils.useSnappyCompression(sparkSession)
		EtlDataService.etlMemberLog(ssc, sparkSession)//清洗用户数据
		EtlDataService.etlBaseAdLog(ssc, sparkSession)//导入基础广告表数据
		EtlDataService.etlBaseWebSiteLog(ssc, sparkSession) //导入基础网站表数据
		EtlDataService.etlMemberRegtypeLog(ssc, sparkSession) //清洗用户注册数据
		EtlDataService.etlMemPayMoneyLog(ssc, sparkSession) //导入用户支付情况记录
		EtlDataService.etlMemVipLevelLog(ssc, sparkSession) //导入vip基础数据
	}
}
