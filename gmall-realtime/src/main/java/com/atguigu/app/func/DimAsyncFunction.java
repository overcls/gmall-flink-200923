package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Date:2021/3/21
 * Description:封装维度异步查询的函数类DimAsyncFunction
 * 该类继承异步方法类RichAsyncFunction，实现自定义维度查询接口
 * 其中RichAsyncFunction<IN,OUT>是Flink提供的异步方法类，此处因为是查询操作输入类型和返回类型一致，所以是<T,T>
 * RichAsyncFunction这个类要实现两个方法：open用于初始化异步连接池，asyncInvoke方法是核心方法，里面的操作必须是异步的，如果查询的数据库有异步API也可以用线程的异步方法，如果没有异步方法，就要利用线程池等方式实现异步查询
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                String key = getKey(input);

                JSONObject dimInfo = DimUtil.getDim(tableName, key);

                if (dimInfo != null && dimInfo.size() > 0) {
                    try {
                        join(input, dimInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
}
