package com.qphone.dmp.biaoqian

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月13日
  *
  * @author 唐枫
  * @version : 1.0
  */
object TagsUtils {
  val IdContidition =
    """
      |imei != "" or imeimd5 != "" or imeisha1 != "" or
      |idfa != "" or idfasha1 != "" or  idfamd5 != "" or
      |mac != "" or macmd5 != "" or macsha1 != "" or
      |androdid != "" or androdidmd5 != "" or androdidsha1 != "" or
      |openudid != "" or openuidmd5 != "" or openuidsha1 != ""
    """.stripMargin

  def getAllUserId(v:Row) : ListBuffer[String] ={
    val userIds = new collection.mutable.ListBuffer[String]()

      if(v.getAs[String]("imei").nonEmpty ) userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
      if(v.getAs[String]("idfa").nonEmpty ) userIds.append("IM:" + v.getAs[String]("idfa").toUpperCase)
      if(v.getAs[String]("mac").nonEmpty ) userIds.append("IM:" + v.getAs[String]("mac").toUpperCase)
      if(v.getAs[String]("androidid").nonEmpty ) userIds.append("IM:" + v.getAs[String]("androidid").toUpperCase)
      if(v.getAs[String]("openudid").nonEmpty ) userIds.append("IM:" + v.getAs[String]("openudid").toUpperCase)

      if(v.getAs[String]("imeimd5").nonEmpty ) userIds.append("IM:" + v.getAs[String]("imeimd5").toUpperCase)
      if(v.getAs[String]("idfamd5").nonEmpty ) userIds.append("IM:" + v.getAs[String]("idfamd5").toUpperCase)
      if(v.getAs[String]("macmd5").nonEmpty ) userIds.append("IM:" + v.getAs[String]("macmd5").toUpperCase)
      if(v.getAs[String]("androididmd5").nonEmpty ) userIds.append("IM:" + v.getAs[String]("androididmd5").toUpperCase)
      if(v.getAs[String]("openudidmd5").nonEmpty ) userIds.append("IM:" + v.getAs[String]("openudidmd5").toUpperCase)

      if(v.getAs[String]("imeisha1").nonEmpty ) userIds.append("IM:" + v.getAs[String]("imeisha1").toUpperCase)
      if(v.getAs[String]("idfasha1").nonEmpty ) userIds.append("IM:" + v.getAs[String]("idfasha1").toUpperCase)
      if(v.getAs[String]("macsha1").nonEmpty ) userIds.append("IM:" + v.getAs[String]("macsha1").toUpperCase)
      if(v.getAs[String]("androididsha1").nonEmpty ) userIds.append("IM:" + v.getAs[String]("androididsha1").toUpperCase)
      if(v.getAs[String]("openudidsha1").nonEmpty ) userIds.append("IM:" + v.getAs[String]("openudidsha1").toUpperCase)
    }


}
