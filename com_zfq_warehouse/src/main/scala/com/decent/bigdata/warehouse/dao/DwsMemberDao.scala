package com.decent.bigdata.warehouse.dao

import org.apache.spark.sql.SparkSession

object DwsMemberDao {
	def getTop3MemberLevelPayMoneyUser(sparkSession: SparkSession,
	                                   dt: String) = {
		sparkSession.sql("select *from(select uid,ad_id,memberlevel,register,appregurl,regsource" +
			",regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)),row_number() over" +
			s" (partition by dn,memberlevel order by cast(paymoney as decimal(10,4)) desc) as rownum,dn from dws.dws_member where dt='${dt}') " +
			" where rownum<4 order by memberlevel,rownum")
	}

	def queryVipLevelCount(sparkSession: SparkSession, dt: String) = {
		sparkSession.sql(s"select vip_level,count(uid),dn,dt from dws.dws_member group where dt='${dt}' group by vip_level,dn,dt")
	}

	def queryMemberLevelCount(sparkSession: SparkSession, dt: String) = {
		sparkSession.sql(s"select memberlevel,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by memberlevel,dn,dt")

	}

	def queryAdNameCount(sparkSession: SparkSession, dt: String) = {
		sparkSession.sql(s"select adname,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by adname,dn,dt")
	}

	def queryRegsourceNameCount(sparkSession: SparkSession, dt: String) = {
		sparkSession.sql(s"select regsourcename,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by regsourcename,dn,dt ")
	}

	def querySiteNameCount(sparkSession: SparkSession, dt: String) = {
		sparkSession.sql(s"select sitename,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by sitename,dn,dt")
	}

	def queryAppregurlCount(sparkSession: SparkSession, dt: Any) = {
		sparkSession.sql(s"select appregurl,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by appregurl,dn,dt")
	}

	/**
	 * 查询用户宽表数据
	 *
	 * @param sparkSession
	 * @return
	 */
	def queryIdlMemberData(sparkSession: SparkSession) = {
		sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
			"siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
	}
}
