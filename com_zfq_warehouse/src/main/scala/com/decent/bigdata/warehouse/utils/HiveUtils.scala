package com.decent.bigdata.warehouse.utils

import org.apache.spark.sql.SparkSession

object HiveUtils {
	/**
	 * 调大最大的分区个数
	 *
	 * @param spark
	 * @return
	 */
	def setMaxpartition(spark: SparkSession) = {
		spark.sql("set hive.exec.dynamic.partition=true")
		spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
		spark.sql("set hive.exec.max.dynamic.partitions=100000")
		spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
		spark.sql("set hive.exec.max.created.files=100000")
	}

	/**
	 * 开启压缩
	 *
	 * @param spark
	 * @return
	 */
	def openCompression(spark: SparkSession) = {
		spark.sql("set mapred.output.compress=true")
		spark.sql("set hive.exec.compress.output=true")
	}

	/**
	 * 开启动态分区和非严格模式
	 *
	 * @param spark
	 * @return
	 */
	def openDynamicPartition(spark: SparkSession) = {
		spark.sql("set hive.exec.dynamic.partition=true")
		spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
	}

	/**
	 * lzo压缩
	 *
	 * @param spark
	 * @return
	 */
	def useLzoCompression(spark: SparkSession) = {
		spark.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")
		spark.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")
	}

	/**
	 * 使用snappy压缩
	 *
	 * @param spark
	 * @return
	 */
	def useSnappyCompression(spark: SparkSession) = {
		spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
		spark.sql("set mapreduce.output.fileoutputformat.compress=true")
		spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
	}
}
