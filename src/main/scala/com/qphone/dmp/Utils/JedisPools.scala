package com.qphone.dmp.Utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Description：$description<br/>
  * Copyright (c) ， 2019， Konfer <br/>
  * This program is protected by copyright laws. <br/>
  * Date：2019年03月14日
  *
  * @author 唐枫
  * @version : 1.0
  */
object JedisPools {
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig(),"192.168.110.111",6379)

  def getJedis = jedisPool.getResource
}
