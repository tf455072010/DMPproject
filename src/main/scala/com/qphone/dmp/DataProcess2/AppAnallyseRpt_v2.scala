package com.qphone.dmp.DataProcess2

import com.qphone.dmp.DataProcess.utils.Log
import com.qphone.dmp.Utils.JedisPools
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月14日
  *
  * @author 唐枫
  * @version : 1.0
  */
object AppAnallyseRpt_v2 {
  object AreaAnalyseRpt{
    def main(args: Array[String]): Unit = {
      if (args.length != 3) {
        println("目录输入错误，返回")
        sys.exit()
      }
      //创建sparkcontext
      val conf: SparkConf = new SparkConf().setAppName("AppAnallyseRpt").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)

      //将文件目录输入
      val Array(inputPath, dictFilePath, outputPath) = args
      //读取字典文件
      val dictMap = sc.textFile(inputPath).map(line => {
        val fields = line.split("\t", -1)
        (fields(4), fields(1))
        //不收集的话只是广播部分数据
      }).collect().toMap
      //将字典数据进行广播
      val bc = sc.broadcast(dictMap)

      //读取数据
      sc.textFile(inputPath).map(_.split(",", -1)).filter(_.length >= 85).map(Log(_))
        .filter(log=> !log.appid.isEmpty || log.appname.isEmpty)
        .mapPartitions(itr=>{

        val jedis = JedisPools.getJedis
          val parList = new collection.mutable.ListBuffer[(String,List[Double])]()
          //遍历分区所有的数据，查询redis（把appname为空的数据进行朱涵换，将结果存放到ListBuffer中
          itr.foreach(log=>{
            var newAppName = log.appname
            if(!StringUtils.isEmpty(newAppName)){
              newAppName = jedis.get(log.appid)
            }
            val req = RptUtils.caculateReq(log.requestmode,log.processnode)
            val rtb = RptUtils.caculateRtb(log.iseffective,log.isbilling,log.isbid,log.adorderid,log.iswin,log.winprice/1000,log.adpayment)
            val showClick = RptUtils.caculateShowClick(log.requestmode,log.iseffective)

            (newAppName,req++rtb++showClick)
            parList +=((newAppName,req++rtb++showClick))
        })
          jedis.close()
        parList.iterator
      }).reduceByKey((list1,list2) => {
        list1.zip(list2).map(t=>t._1 + t._2)
      }).map(t=>t._1 + "," + t._2.mkString(",")).saveAsTextFile(outputPath)

    }



    }
}
