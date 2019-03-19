package com.qphone.dmp.Utils

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月11日
  *
  * @author 唐枫
  * @version : 1.0
  */
object DFUtils {
  def toInt(str:String):Int={
    try {
      str.toInt
    } catch {
      case _ : Exception => 0
    }
  }
  def toDouble(str: String):Double={
    try {
      str.toDouble
    } catch {
      case _ : Exception => 0.0
    }
  }

}
