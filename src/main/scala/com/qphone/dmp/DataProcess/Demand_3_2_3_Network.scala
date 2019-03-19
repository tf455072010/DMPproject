package com.qphone.dmp.DataProcess

import java.util.Properties

import com.qphone.dmp.Utils.{DFUtils, logSchema}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

/**
  * Description：网络类数据统计<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月12日
  *
  * @author 唐枫
  * @version : 1.0
  */
object Demand_3_2_3_Network {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录输入错误，退出！！！")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.setAppName(this.getClass.getSimpleName)
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //设置sql的压缩方式
    sQLContext.setConf("spark.io.compression.codec","snappy")
    val lines = sc.textFile(inputPath)
    var rdd = lines.map(t=>t.split(",",t.length)).filter(_.length >=85).map(arr=>{
      Row(
        arr(0),
        DFUtils.toInt(arr(1)),
        DFUtils.toInt(arr(2)),
        DFUtils.toInt(arr(3)),
        DFUtils.toInt(arr(4)),
        arr(5),
        arr(6),
        DFUtils.toInt(arr(7)),
        DFUtils.toInt(arr(8)),
        DFUtils.toDouble(arr(9)),
        DFUtils.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        DFUtils.toInt(arr(17)),
        arr(18),
        arr(19),
        DFUtils.toInt(arr(20)),
        DFUtils.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        DFUtils.toInt(arr(26)),
        arr(27),
        DFUtils.toInt(arr(28)),
        arr(29),
        DFUtils.toInt(arr(30)),
        DFUtils.toInt(arr(31)),
        DFUtils.toInt(arr(32)),
        arr(33),
        DFUtils.toInt(arr(34)),
        DFUtils.toInt(arr(35)),
        DFUtils.toInt(arr(36)),
        arr(37),
        DFUtils.toInt(arr(38)),
        DFUtils.toInt(arr(39)),
        DFUtils.toDouble(arr(40)),
        DFUtils.toDouble(arr(41)),
        DFUtils.toInt(arr(42)),
        arr(43),
        DFUtils.toDouble(arr(44)),
        DFUtils.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        DFUtils.toInt(arr(57)),
        DFUtils.toDouble(arr(58)),
        DFUtils.toInt(arr(59)),
        DFUtils.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        DFUtils.toInt(arr(73)),
        DFUtils.toDouble(arr(74)),
        DFUtils.toDouble(arr(75)),
        DFUtils.toDouble(arr(76)),
        DFUtils.toDouble(arr(77)),
        DFUtils.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        DFUtils.toInt(arr(84))
      )
    })


    //将schema信息关联到rdd上
    var df: DataFrame = sQLContext.createDataFrame(rdd,logSchema.schema)
    //创建临时表
    df.createTempView("AllData")
    val load = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("password",load.getString("jdbc.password"))

    val regional = sQLContext.sql(
      """
        |select networkmannername,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode =3 then 1 else 0 end) ggrequest,
        |sum(case when iseffective = 1 and isbilling =1 and isbid = 1 then 1 else 0 end) cyrequest,
        |sum(case when iseffective = 1 and isbilling =1 and iswin = 1 and adorderid != 0 then 1 else 0 end) jjrequest,
        |sum(case when requestmode = 2 and iseffective =1 then 1 else 0 end) zsrequest,
        |sum(case when requestmode = 3 and iseffective =1 then 1 else 0 end) djrequest,
        |sum(case when iseffective = 1 and isbilling >=1 and iswin = 1 then 1 else 0 end)/1000 dspspend,
        |sum(case when iseffective = 1 and isbilling >=1 and iswin = 1 then 1 else 0 end)/1000 dspcost
        |from AllData group by networkmannername
        |
      """.stripMargin
    )
    //网络类数据存储到jdbc上
    //    regional.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.NetTable"),pro)

    //设备类数据处理
    val equip = sQLContext.sql(
      """
        |select networkmannername,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode =3 then 1 else 0 end) ggrequest,
        |sum(case when iseffective = 1 and isbilling =1 and isbid = 1 then 1 else 0 end) cyrequest,
        |sum(case when iseffective = 1 and isbilling =1 and iswin = 1 and adorderid != 0 then 1 else 0 end) jjrequest,
        |sum(case when requestmode = 2 and iseffective =1 then 1 else 0 end) zsrequest,
        |sum(case when requestmode = 3 and iseffective =1 then 1 else 0 end) djrequest,
        |sum(case when iseffective = 1 and isbilling >=1 and iswin = 1 then 1 else 0 end)/1000 dspspend,
        |sum(case when iseffective = 1 and isbilling >=1 and iswin = 1 then 1 else 0 end)/1000 dspcost
        |from AllData group by networkmannername
        |
      """.stripMargin
    )


  }

}
