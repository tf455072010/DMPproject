package com.qphone.dmp.biaoqian

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月13日
  *
  * @author 唐枫
  * @version : 1.0
  */
object Tags4ctx {
  def main(args: Array[String]): Unit = {
    if(args.length!=4){
      println(
        """
          |参数：输入路径
          |字典文件路径
          |停用词库
          |输出路径
        """.stripMargin)
      sys.exit()
    }
    val Array(inputPath,dictFilePath,stopWordFilePath,outputPath) = args

    val conf = new SparkConf()
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.setAppName(this.getClass.getSimpleName)
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)


    //读取字典文件--appMapping
    val dictMap = sc.textFile(dictFilePath).map(line=>{

      val fields = line.split("\t",-1)
      if(fields.length < 5){
        (0, 0)
      } else {
        (fields(4), fields(0))
      }
    }).collect().toMap

    //stopWords
     val stopWordsMap = sc.textFile(dictFilePath).map((_,0)).collect().toMap

    //将数据进行广播
    val BroadDict = sc.broadcast(dictMap)
    val BraodStopWords = sc.broadcast(stopWordsMap)

    val load = ConfigFactory.load()
    val hbTableName = load.getString("hbase.table.name")

    //判断hbase中的表是否存在，不存在则创建
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum","hbase.zookeeper.host")
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbAdmin = hbConn.getAdmin

    if (hbAdmin.tableExists(TableName.valueOf(hbTableName)))

    import sQLContext.implicits._

    //读取日志parquet文件
    sQLContext.read.parquet(inputPath).where(
      """
        |imei != "" or imeimd5 != "" or imeisha1 != "" or
        |idfa != "" or idfamd5 != "" or idfasha1 != "" or
        |mac != "" or macmd5 != "" or macsha1 != "" or
        |androidid != "" or androididmd5 != "" or androididsha1 != "" or
        |openudid != "" or openudidmd5 != "" or openudidsha1 != ""
      """.stripMargin).map(row=>{
      //行数据进行标签化处理
      val ads = Tags4Ads.makeTags(row)
      val apps = Tag4App.makeTags(row,BroadDict.value)
      val devices = Tags4Device.makeTags(row)
      val keyWords = Tags4KeyWords.makeTags(row,BraodStopWords.value)

      val allUserId = TagsUtils.getAllUserId(row)

      (allUserId(0),(ads++apps ++devices++keyWords).toList)
    }).rdd.reduceByKey((a,b) => {
      //List(("K电视剧"，1)，("K电视剧",1)) => groupby => Map["K电视剧",List(("K电视剧",1),("K电视剧",1))
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
//      (a++b).groupBy(_._1).map{
//        case (k,sameTags) => (k,sameTags.map(_._2).sum)
//      }.toList
    })
      .map{
        case (userId,userTags) =>{
          val put = new Put(Bytes.toBytes(userId))
          val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
          put.addColumn()
        }
      }
    //存储到普通路径
//      .saveAsTextFile(outputPath)

  }

}
