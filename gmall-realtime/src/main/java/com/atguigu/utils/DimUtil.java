package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Date:2021/3/20
 * Description:
 */
public class DimUtil {
    /**
     *
     * @param table
     * @param value
     * @return
     */
    public static JSONObject getDim(String table ,String value){
        Jedis jedis = RedisUtil.getjedis();

        String redisKey = table + ":" +value;

        String dimJson = jedis.get(redisKey);

        if(dimJson != null && dimJson.length() > 0){
            jedis.close();
            return JSON.parseObject(dimJson);
        }

        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id = '" + value + "'";

        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class, false);

        JSONObject jsonObject = queryList.get(0);

        jedis.set(redisKey,jsonObject.toJSONString());
        jedis.expire(redisKey,24*60*60);

        jedis.close();
        return jsonObject;
    }

    public static void deleteCache(String key){
        Jedis jedis = RedisUtil.getjedis();
        jedis.del(key);
        jedis.close();
    }
}
