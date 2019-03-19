package com.qphone.dmp.biaoqian

import org.apache.spark.sql.Row

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月16日
  *
  * @author 唐枫
  * @version : 1.0
  */
object Tags4KeyWords extends Tags {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Map[String,Int]]

    val kws = row.getAs[String]("keywords")

    kws.split("\\|").filter(kw => kw.trim.length >= 3 && kw.trim.length <= 8 && !stopWords.contains(kw))
      .foreach(kw => map += "K" + kw -> 1 )

    map
  }
}
