package com.qphone.dmp.DataProcess2



/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月13日
  *
  * @author 唐枫
  * @version : 1.0
  */
/**
  * List（原始请求，有效请求，广告请求）
  */
object RptUtils {
  def caculateReq(reqMode:Int,proMode:Int):List[Double]={

    if(reqMode ==1 && proMode ==1 ) {
      List[Double](1,0,0)
    }else if(reqMode ==1 && proMode ==2 ) {
      List[Double](1,1,0)
    }else if(reqMode ==1 && proMode ==3 ) {
      List[Double](1,1,1)
    }else List[Double](0,0,0)


  }

  /**
    * List(参与竞价，竞价成功，消费，成本）
    */
  def caculateRtb(effective:Int,isBilling:Int,isBid:Int,adorderId:Int,isWin:Int,winprice:Double,adpayment:Double):List[Double]={

    if(effective ==1 && isBilling ==1 && isWin == 1){
      List[Double](0,1,winprice/1000.0,adpayment/1000.0)
    }else if(effective ==1 && isBilling ==1 && isWin == 1 && adorderId != 0){
      List[Double](1,0,0,0)
    }else List[Double](0,0,0,0)


  }

  /**
    * List(展示数，点击数)
    */
  def caculateShowClick(reqMode:Int,effective:Int):List[Double]={

    if(reqMode ==2 && effective ==1){
      List[Double](1,0)
    }else if(reqMode ==3 && effective ==1){
      List[Double](0,1)
    }else List[Double](0,0)

  }


}
