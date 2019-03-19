package com.qphone.dmp.biaoqian

import org.apache.commons.lang3.StringUtils
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
object Tag4App extends Tags {
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val appDict = args(1).asInstanceOf[Map[String,String]]

    //广告位类型和名称

    val appId = row.getAs[String]("appid")
    val appName = row.getAs[String]("appname")

    if(StringUtils.isEmpty(appName)){
      appDict.contains(appId) match {
        case true => map += "APP" +appDict.get(appId) -> 1
      }
    }
    map
  }
}
