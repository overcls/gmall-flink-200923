package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Date:2021/3/20
 * Description:
 * 数据流：Web/App>>Nginx>>SpringBoot>>Kafka(ods)>>FlinkApp>>Kafka(dwd)>>FlinkApp>>Kafka(dwn)
 * 进程：MockLog>>Nginx>>Logger>>Kafka(Zookeeper)>>BaseLogApp>>Kafka>>UserJumpDetailApp>>Kafka
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //CheckPoint
/*      env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flnik/checkpoint"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/
        //TODO 2 从Kafka dwd_page_log 主题读取数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app_group";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, sourceTopic);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {

                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(watermarkStrategy);

        //TODO 4 按照Mid分组
        KeyedStream<JSONObject, String> jsonObjKS = jsonObjWithWMDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //TODO 5 定义模式序列
        //{"common":{"ar":"440000","ba":"Xiaomi","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_1","os":"Android 9.0","uid":"26","vc":"v2.1.134"},"page":{"during_time":4385,"item":"1,6","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},"ts":1615862194000}
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .followedBy("follow")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId != null && lastPageId.length() > 0;
                    }
                })
                .within(Time.seconds(3));

        //TODO 6 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjKS, pattern);

        //TODO 7 提取超时事件
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("TimeOut") {
        };

        SingleOutputStreamOperator<JSONObject> resultDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                List<JSONObject> objectList = map.get("begin");
                return objectList.get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return null;
            }
        });

        //TODO 8 提取侧输出流数据写入Kafka主题
        DataStream<JSONObject> outputDS = resultDS.getSideOutput(outputTag);
        outputDS.print(">>>>>");
        outputDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 9 启动任务
        env.execute();
    }
}
