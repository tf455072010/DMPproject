package com.qphone.dmp.DataProcess2


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月13日
  *
  * @author 唐枫
  * @version : 1.0
  */
object AreaAnalyseRpt{
  def main(args: Array[String]): Unit = {
    if(args.length !=2){
      println("目录输入错误，返回")
      sys.exit()
    }
    //创建sparkcontext
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //创建sqlcontext
    val sQLContext = new SQLContext(sc)
    //设置sql压缩方式
    sQLContext.setConf("spark.io.compression.codec","snappy")

    //将文件目录输入
    val Array(inputPath,outputPath) = args

    val parquetData = sQLContext.read.parquet(inputPath)
    import sQLContext.implicits._
    parquetData.map(row=>{

      //是不是原始请求 有效请求，广告请求，需要两个字段REQUESTMODE,PROCESSNODE
      val reqMode = row.getAs[Int]("requestmode")
      val proMode = row.getAs[Int]("processmode")
      val effective = row.getAs[Int]("iseffective")
      val isBilling = row.getAs[Int]("isbilling")
      val isBid = row.getAs[Int]("isbid")
      val adorderId = row.getAs[Int]("adorderid")
      val isWin = row.getAs[Int]("iswin")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val reqList = RptUtils.caculateReq(reqMode,proMode)
      val rtbList = RptUtils.caculateRtb(effective,isBilling,isBid,adorderId,isWin,winprice,adpayment)
      val showClickList = RptUtils.caculateShowClick(reqMode,effective)

      //返回元组
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),reqList++rtbList++showClickList)

    }).rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1._1+","+t._1._2+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)

    sc.stop()
  }





}
