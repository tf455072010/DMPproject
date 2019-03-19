package com.qphone.dmp.biaoqian

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description：广告标签相关<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月13日
  *
  * @author 唐枫
  * @version : 1.0
  */
object Tags4Ads extends Tags {
  /**
    * 打标签的方法定义
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]

    //广告位类型和名称
    val adTypeId = row.getAs[Int]("adspacetype")
    val adTypeName = row.getAs[String]("adsapcetypename")


    if(adTypeId > 9) map += "LC" + adTypeId -> 1
    else if(adTypeId > 0) map += "LC0" +adTypeId -> 1

    if(StringUtils.isNoneEmpty(adTypeName)) map += "LN" + adTypeName -> 1
    //渠道相关
    val adpId = row.getAs[String]("adplatformproviderid")
    if(adpId > 0) map+= (("CN"+adpId,1))


    map

  }
}
