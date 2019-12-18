package com.atguigu.warehouse.test

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("data_etl")
			.master("local[*]")
			//开启hive的支持
			.enableHiveSupport()
			.getOrCreate()

		val sc = spark.sparkContext
		//sc.hadoopConfiguration.addResource("core-site.xml")
		//sc.hadoopConfiguration.addResource("hdfs-site.xml")
		//引入的sparkSQL隐式转换中spark不是包名，是上下文环境的对象名称
		import spark.implicits._


		val inputJson: DataFrame = spark.read.json("hdfs://nameservice1/user/atguigu/ods/member.log")
		//过滤数据
		val member: Dataset[Member] = inputJson.as[Member]
		val hiveSet: Dataset[Hive_member] = member.map(data => {
			val tuple: (String, String) = data.phone.splitAt(3)
			val tail: String = tuple._2.splitAt(5)._2
			val tel = tuple._1 + "****" + tail
			val hive = Hive_member(data.uid.toInt, data.ad_id.toInt, data
				.birthday,
				data.email, data.fullname.substring(0, 1) + "xx", data
					.iconurl, data
					.lastlogin, data.mailaddr, data.memberlevel, data.password
					.replace(data.password, "******"), data.paymoney, data.paymoney,
				tel, data.qq, data.register, data.unitname, data.userip,
				data.zipcode, data.dt, data.dn)
			hive
		})
		//将过滤好的数据导入到hive表中

		//从hive中做相应的查询操作

		hiveSet.show()
		sc.stop()
	}
}
case class Member(ad_id: String, birthday: String, dn: String,
                  dt: String, email: String, fullname: String,
                  iconurl: String, lastlogin: String, mailaddr: String,
                  memberlevel: String, password: String, paymoney: String,
                  phone: String, qq: String, register: String, regupdatetime: String,
                  uid: String, unitname: String, userip: String, zipcode: String
                 )

case class Hive_member(uid: Int, ad_id: Int, birthday: String,
                       email: String, fullname: String,
                       iconurl: String, lastlogin: String, mailaddr: String,
                       memberlevel: String, password: String, paymoney: String,
                       phone: String, qq: String, register: String, regupdatetime: String,
                       unitname: String, userip: String, zipcode: String,
                       dt: String, dn: String
                      )