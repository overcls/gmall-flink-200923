package com.atguigu.app.dwm;

/**
 * Date:2021/3/19
 * Description:
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * 数据流: Web/App -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(ZK) -> FlinkApp -> Kafka
 * 进程  : MockLog -> nginx -> Logger     -> Kafka      -> BaseLogApp -> Kafka   -> UniqueVisitorApp -> Kafka
 */
public class UniqueVisitorApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*
        //开启CheckPoint，设置每个CheckPoint的间隔时间
        env.enableCheckpointing(5000L);
        //设置CheckPoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //设置CheckPoint的模式为精准一次消费，避免丢失和重复数据
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置CheckPoint的重试策略为3次，每次失败后等待5000ms
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //设置CheckPoint保存路径
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint"));
        //在IDEA里运行，需要设置登录名才能在HDFS上创建文件夹，避免权限问题
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/
        //TODO 2 读取 dwd_page_log 主题数据创建流
        String group_Id = "unique_visitor_app_group";
        String topic = "dwd_page_log";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(group_Id, topic));

        //TODO 3 将每行数据转换JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4 按照Mid分组
        KeyedStream<JSONObject, String> jsonObjWithMidKS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5 使用状态编程的方式对数据做过滤  富函数、Process
        SingleOutputStreamOperator<JSONObject> dauDS = jsonObjWithMidKS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-date", String.class);

                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.days(1))
                        //可以设置时间类型（事件时间，进入Flink时间，处理时间）
//                          .setTtlTimeCharacteristic()
                        //设置重置时间类型（Disabled:不重置,OnCreateAndWrite:创建和有写入时重置,OnReadAndWrite:被读取时和有写入时重置）
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                //设置存活时间
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                valueState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                //取出上一个页面的数据
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                //没有上一个页面的数据
                if (lastPageId == null || lastPageId.length() <= 0) {

                    //取出状态数据
                    String visitData = valueState.value();

                    String date = sdf.format(jsonObject.getLong("ts"));

                    if (visitData == null || visitData.length() <= 0 || !visitData.equals(date)) {
                        valueState.update(date);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //TODO 6 将数据写入DWM层主题
        dauDS.print(">>>>>");
        dauDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));
        //TODO 7 启动任务
        env.execute();

    }

}