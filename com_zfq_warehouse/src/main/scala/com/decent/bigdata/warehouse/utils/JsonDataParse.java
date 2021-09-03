package com.decent.bigdata.warehouse.utils;

import com.alibaba.fastjson.JSONObject;

/**
 * JsonDataParse
 *
 * @author zhaofuqiang
 * @date 2019.12.17 17:08
 */
public class JsonDataParse {
  /**
   * 解析将字符串解析成json对象
   *
   * @param data
   * @return
   */
  public static JSONObject getData(String data) {
    try {
      return JSONObject.parseObject(data);
    } catch (Exception e) {
      return null;
    }
  }
}
