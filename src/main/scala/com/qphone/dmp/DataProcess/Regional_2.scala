//package com.qphone.dmp.DataProcess
//
//import com.qphone.dmp.Utils.RptUtils
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
//  * Description：$description<br/>
//  * Copyright (c) ， 2019， Konfer <br/>
//  * This program is protected by copyright laws. <br/>
//  * Date：2019年03月12日
//  *
//  * @author 唐枫
//  * @version : 1.0
//  */
//object Regional_2 {
//  def main(args: Array[String]): Unit = {
//    if(args.length != 2){
//      println("目录输入错误，退出！！！")
//      sys.exit()
//    }
//    val Array(inputPath,outputPath) = args
//    val conf = new SparkConf().setMaster("local[*]")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    conf.setAppName(this.getClass.getSimpleName)
//    conf.setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val sQLContext = new SQLContext(sc)
//    //设置sql的压缩方式
//    sQLContext.setConf("spark.io.compression.codec","snappy")
//    val lines = sQLContext.read.parquet(inputPath)
//    val value = lines.map(row=>{
//      //先获取基本数据——原始请求，有效请求，广告请求
//      val requestmode = row.getAs[Int]("requestmode")
//      val processnode = row.getAs[Int]("processnode")
//      //参与竞价数，成功数，展示数，点击数
//      val iseffective = row.getAs[Int]("iseffective")
//      val isbilling = row.getAs[Int]("isbilling")
//      val isbid = row.getAs[Int]("isbid")
//      val iswin = row.getAs[Int]("iswin")
//      val adorderid = row.getAs[Int]("adorderid")
//      val winprice = row.getAs[Double]("winprice")
//      val ad = row.getAs[Double]("adpayment")
//      //编写业务方法，调用原始请求，有效请求，广告请求
//      val reqlist = RptUtils.caculateReq(requestmode,processnode)
//      //参与竞价数，成功数，展示数，点击数
//      val adlist = RptUtils.caculateRtb(iseffective,isbilling,isbid,iswin,adorderid,winprice,ad)
//      // 点击数 展示数
//      val adCountlist = RptUtils.caculateShowClick(requestmode,iseffective)
//      // 取值地域维度
//      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),
//        reqlist ++ adlist ++ adCountlist
//      )
//    })
//      value.reduceByKey((list1,list2)=>{
//      // list1(0,1,1,0) list2(1,1,1,1) zip((0,1),(1,1),(1,1),(0,1))
//      list1.zip(list2).map(t=>t._1+t._2)
//    })// 调整下方位
//      .map(t=>t._1._1+" , "+ t._1._2 +" , "+t._2.mkString(","))
//      // 将结果数据存入hdfs
//      .saveAsTextFile(outputPath)
//
//  }
//
//
//}
