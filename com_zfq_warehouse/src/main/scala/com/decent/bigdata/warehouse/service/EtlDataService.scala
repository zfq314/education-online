package com.decent.bigdata.warehouse.service

import com.alibaba.fastjson.JSONObject
import com.decent.bigdata.warehouse.utils.JsonDataParse
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {
	/**
	 * 导入vip基础数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
		import sparkSession.implicits._
		val result = ssc.textFile("/user/atguigu/ods/pcenterMemViplevel.log")
			.filter(data => {
				val jsonObject = JsonDataParse.getData(data)
				jsonObject.isInstanceOf[JSONObject]
			}).mapPartitions(partition => {
			partition.map(data => {
				val jsonObject = JsonDataParse.getData(data)
				val discountval = jsonObject.getString("discountval")
				val end_time = jsonObject.getString("end_time")
				val last_modify_time = jsonObject.getString("last_modify_time")
				val max_free = jsonObject.getString("max_free")
				val min_free = jsonObject.getString("min_free")
				val next_level = jsonObject.getString("next_level")
				val operator = jsonObject.getString("operator")
				val start_time = jsonObject.getString("start_time")
				val vip_id = jsonObject.getIntValue("vip_id")
				val vip_level = jsonObject.getString("vip_level")
				val dn = jsonObject.getString("dn")
				(vip_id, vip_level, start_time, end_time, last_modify_time,
					max_free, min_free, next_level, operator, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")

	}

	/**
	 * 导入用户支付情况记录
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
		import sparkSession.implicits._
		val result = ssc.textFile("/user/atguigu/ods/pcentermempaymoney.log")
			.filter(data => {
				val jsonObject = JsonDataParse.getData(data)
				jsonObject.isInstanceOf[JSONObject]
			}).mapPartitions(partition => {
			partition.map(data => {
				val jsonObject = JsonDataParse.getData(data)
				val uid = jsonObject.getIntValue("uid")
				val paymoney = jsonObject.getString("paymoney")
				val siteid = jsonObject.getIntValue("siteid")
				val vip_id = jsonObject.getIntValue("vip_id")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(uid, paymoney, siteid, vip_id, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd" +
			".dwd_pcentermempaymoney")
	}

	/**
	 * 清洗用户注册数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
		import sparkSession.implicits._
		val result = ssc.textFile("/user/atguigu/ods/memberRegtype.log")
			.filter(data => {
				val jsonObject = JsonDataParse.getData(data)
				jsonObject.isInstanceOf[JSONObject]
			}).mapPartitions(partition => {
			partition.map(data => {
				val jsonObject = JsonDataParse.getData(data)
				val appkey = jsonObject.getString("appkey")
				val appregurl = jsonObject.getString("appregurl")
				val dbp_uuid = jsonObject.getString("dbp_uuid")
				val createtime = jsonObject.getString("createtime")
				val isranreg = jsonObject.getString("isranreg")
				val regsource = jsonObject.getString("regsource")
				val regsourceName = regsource match {
					case "1" => "PC"
					case "2" => "Mobile"
					case "3" => "App"
					case "4" => "WeChat"
					case _ => "other"
				}
				val uid = jsonObject.getIntValue("uid")
				val websiteid = jsonObject.getIntValue("websiteid")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(uid, appkey, appregurl, dbp_uuid, createtime, isranreg, regsource,
					regsourceName, websiteid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd" +
			".dwd_member_regtype")

	}

	/**
	 * 导入基础网站表数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
		import sparkSession.implicits._
		val result = ssc.textFile("/user/atguigu/ods/baswewebsite.log").filter(
			data => {
				val jsonObject = JsonDataParse.getData(data)
				jsonObject.isInstanceOf[JSONObject]
			}
		).mapPartitions(partition => {
			partition.map(data => {
				val jsonObject = JsonDataParse.getData(data)
				val siteid = jsonObject.getIntValue("siteid")
				val sitename = jsonObject.getString("sitename")
				val siteurl = jsonObject.getString("siteurl")
				val delete = jsonObject.getIntValue("delete")
				val createtime = jsonObject.getString("createtime")
				val creator = jsonObject.getString("creator")
				val dn = jsonObject.getString("dn")
				(siteid, sitename, siteurl, delete, createtime, creator, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd" +
			".dwd_base_website")

	}

	/**
	 * 清洗用户数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {
		//调用隐式转换
		import sparkSession.implicits._
		ssc.textFile("/user/atguigu/ods/member.log")
			.filter(data => {
				val obj = JsonDataParse.getData(data)
				obj.isInstanceOf[JSONObject]
			}).mapPartitions(partition => {
			partition.map(data => {
				val jsonObject: JSONObject = JsonDataParse.getData(data)
				val ad_id = jsonObject.getIntValue("ad_id")
				val birthday = jsonObject.getString("birthday")
				val email = jsonObject.getString("email")
				val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
				val iconurl = jsonObject.getString("iconurl")
				val lastlogin = jsonObject.getString("lastlogin")
				val mailaddr = jsonObject.getString("mailaddr")
				val memberlevel = jsonObject.getString("memberlevel")
				val password = "******"
				val paymoney = jsonObject.getString("paymoney")
				val phone = jsonObject.getString("phone")
				val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
				val qq = jsonObject.getString("qq")
				val register = jsonObject.getString("register")
				val regupdatetime = jsonObject.getString("regupdatetime")
				val uid = jsonObject.getIntValue("uid")
				val unitname = jsonObject.getString("unitname")
				val userip = jsonObject.getString("userip")
				val zipcode = jsonObject.getString("zipcode")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(uid, ad_id, birthday, email, fullname, iconurl, lastlogin,
					mailaddr, memberlevel, password, paymoney, newphone, qq, register,
					regupdatetime, unitname, userip, zipcode, dt, dn)
			})
		}).toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd" + ".dwd_member")
	}

	/**
	 * 导入基础广告表数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
		import sparkSession.implicits._ //隐士转换
		val result = ssc.textFile("/user/atguigu/ods/baseadlog.log")
			.filter(data => {
				val obj = JsonDataParse.getData(data)
				obj.isInstanceOf[JSONObject]
			}).mapPartitions(partition => {
			partition.map(data => {
				val jsonObject = JsonDataParse.getData(data)
				val adid = jsonObject.getIntValue("adid")
				val adname = jsonObject.getString("adname")
				val dn = jsonObject.getString("dn")
				(adid, adname, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd" +
			".dwd_base_ad")
	}
}
