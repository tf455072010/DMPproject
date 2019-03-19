package com.qphone.dmp.DataProcess2

import com.qphone.dmp.Utils.JedisPools
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description：将数据保存到redis中<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月14日
  *
  * @author 唐枫
  * @version : 1.0
  */
object AppDict2Redis {

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
    val dictMap = sc.textFile(dictFilePath).map(line => {
      val fields = line.split("\t", -1)
      (fields(4), fields(1))
      //不收集的话只是广播部分数据
    }).foreachPartition(itr => {
      val jedis = JedisPools.getJedis
      itr.foreach(t =>
        jedis.set(t._1, t._2)
      )
      jedis.close()
    })


  }
}
