package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * Date:2021/3/21
 * Description:自定义维度查询接口
 * 这个异步维度查询的方法适用于各种维度表的查询，用什么条件查，查出来的结果如何合并到数据流对象中，需要使用者自己定义
 */
public interface DimJoinFunction<T> {

    //获取数据中的所要的关联的维度的主题
    String getKey (T input);

    //关联事实数据和维度数据
    void join(T input, JSONObject dimInfo) throws Exception;
}
