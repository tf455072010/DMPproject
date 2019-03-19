package com.qphone.dmp.Utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Description：<br/>
 * Copyright (c) ， 2019， Konfer <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年03月13日
 *
 * @author 唐枫
 * @version : 1.0
 */
public class GeoCodingApi {

    private GeoCodingApi(){}

    private static Logger logger = Logger.getLogger(GeoCodingApi.class);

    // 对Map内所有value作utf8编码，拼接返回结果
    private static String toQueryString(Map<?, ?> data) throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Map.Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),"UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    private static String MD5(String md5) throws NoSuchAlgorithmException {
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
        byte[] array = md.digest(md5.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < array.length; ++i) {
            sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
        }
        return sb.toString();
    }

    private static String sn(String paramsStr) {

        try {
            // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
            String wholeStr = new String("/geocoder/v2/?" + paramsStr + "Qs8i14YPn80g2Ohx2Lj5tb3Bsb6dD7Ps");

            // 对上面wholeStr再作utf8编码
            String tempStr = URLEncoder.encode(wholeStr, "UTF-8");
            return MD5(tempStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据经纬度返回商业兴趣标签
     * @param latAndLng 纬度,经度
     * @return
     */
    public static String bussinessTagFromBaidu(String latAndLng){
        // 最终需要返回的标签
        StringBuffer businessTags = new StringBuffer();
        HttpClient httpClient = new HttpClient();
        GetMethod method = null;
        try {
            Map paramsMap = new LinkedHashMap<String, String>();
            paramsMap.put("callback", "renderReverse");
            paramsMap.put("location", latAndLng); //纬度, 经度
            paramsMap.put("output", "json");
            paramsMap.put("pois", "1"); //是否显示指定位置周边的poi，0为不显示，1为显示。当值为1时，默认显示周边1000米内的poi。
            // paramsMap.put("radius", "500"); // poi召回半径，允许设置区间为0-1000米，超过1000米按1000米召回。
            paramsMap.put("ak", "Fg79IGFnhqlZy9IHvXxkLmp1EBASAlzI");
            String paramsStr = toQueryString(paramsMap);
            String finalURL = "http://api.map.baidu.com/geocoder/v2/?"+paramsStr + "&sn="+sn(paramsStr);

            logger.debug(finalURL);

            method = new GetMethod(finalURL);
            int i = httpClient.executeMethod(method);
            if (i == 200) {
                String bodyAsString = method.getResponseBodyAsString();

                if (!bodyAsString.startsWith("{")) {
                    bodyAsString = bodyAsString.replace("renderReverse&&renderReverse(", "");
                    bodyAsString = bodyAsString.substring(0, bodyAsString.length() - 1);
                }

                logger.debug(bodyAsString);

                // json 解析结果数据, 解析出来bussiness
                JSONObject parseObject = JSONObject.parseObject(bodyAsString);
                int status = parseObject.getIntValue("status");
                if (status == 0) {

                    JSONObject result = parseObject.getJSONObject("result");
                    businessTags.append(result.getString("business"));

                    if (businessTags.length() == 0) {
                        JSONArray jsonArray = result.getJSONArray("pois");
                        if (jsonArray.size() > 0) {
                            JSONObject object = jsonArray.getJSONObject(0);
                            businessTags.append(object.getString("tag"));
                        }
                    }
                }

            }
        } catch (Exception e) {
            logger.error(e.fillInStackTrace() + e.getMessage());
        } finally {
            if (null!= method) method.releaseConnection();
        }
        return businessTags.toString();
    }

    public static void main(String[] args) {
        try {
            HttpClient httpClient = new HttpClient();
            // 计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，该方法根据key的插入顺序排序；
            // post请使用TreeMap保存<key,value>，该方法会自动将key按照字母a-z顺序排序。
            // 所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
            // 以get请求为例：
            // http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
            Map paramsMap = new LinkedHashMap<String, String>();
            paramsMap.put("callback", "renderReverse");
            paramsMap.put("location", "47.008052,130.723014"); //纬度, 经度47.008052,130.723014
            paramsMap.put("output", "json");
            paramsMap.put("pois", "1"); //是否显示指定位置周边的poi，0为不显示，1为显示。当值为1时，默认显示周边1000米内的poi。
            // paramsMap.put("radius", "500"); // poi召回半径，允许设置区间为0-1000米，超过1000米按1000米召回。
            paramsMap.put("ak", "Fg79IGFnhqlZy9IHvXxkLmp1EBASAlzI");
            String paramsStr = toQueryString(paramsMap);
            String finalURL = "http://api.map.baidu.com/geocoder/v2/?"+paramsStr + "&sn="+sn(paramsStr);


            GetMethod method = new GetMethod(finalURL);
            int i = httpClient.executeMethod(method);
            if (i == 200) {
                String bodyAsString = method.getResponseBodyAsString();
                method.releaseConnection();

                if (!bodyAsString.startsWith("{")) {
                    bodyAsString = bodyAsString.replace("renderReverse&&renderReverse(", "");
                    bodyAsString = bodyAsString.substring(0, bodyAsString.length() - 1);
                }

                System.out.println("bodyAsString = " + bodyAsString);

                // json 解析结果数据, 解析出来bussiness
                JSONObject parseObject = JSONObject.parseObject(bodyAsString);
                int status = parseObject.getIntValue("status");
                if (status == 0) {

                    JSONObject result = parseObject.getJSONObject("result");
                    String businessTags = result.getString("business");

                    if (StringUtils.isEmpty(businessTags)) {
                        JSONArray jsonArray = result.getJSONArray("pois");
                        if (jsonArray.size() > 0) {
                            JSONObject object = jsonArray.getJSONObject(0);
                            businessTags = object.getString("tag");
                        }
                    }

                    System.out.println("businessTags = " + businessTags);
                } else {
                    System.err.println("error: " +status);
                }

            } else {
                System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
