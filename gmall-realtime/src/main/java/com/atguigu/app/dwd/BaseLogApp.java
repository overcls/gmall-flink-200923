package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * Date:2021/3/17
 * 数据流：Web/App>>Nginx>>SpringBoot>>Kafka(ods_base_log)>>Flink>>Kafka(dwd_page/start/display_log)
 * 框架服务：MockLog>>Nginx>>Logger.sh>>Kafka(ods)>>BaseLogApp>>Kafka(dwd)
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度应该与Kafka主题的分区数相同
        env.setParallelism(1);
/*
        //设置CheckPoint相关参数
        env.enableCheckpointing(5000L);
        //设置CheckPoint的模式为精准一次消费
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置CheckPoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(20000L);
        //设置CheckPoint的重试策略为：重试三次，每次延迟1s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L));
        //设置状态后端，存放到HDFS上
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));
        //伪装成atguigu用户，解决操作权限问题
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/

        //TODO 2 读取Kafka ods_base_log 主题数据创建流
        String groupId = "base_log_app_group";
        String topic = "ods_base_log";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(groupId, topic);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3 新老用户校验 (分组 状态编程 富函数)
        //分离出脏数据
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });
        KeyedStream<JSONObject, String> jsonObjWithMidKeyDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjWithMidKeyDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            //声明状态
            private ValueState<String> firstVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visit-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                //获取新老用户标记
                String str = jsonObject.getJSONObject("common").getString("is_new");

                /**
                 * "1"表示该用户是新用户，但可能出现误判
                 * 因为在启动日志发送到服务器的时候，只是在和设备本地保存的数据惊醒了对比
                 * 如果本地存在该用户登录过的数据，那自然就不是新用户，标记就是"0"
                 * 但是当本地数据被清理后，由于未能和服务器完成完整的通信，所以不能获取该用户的实际状态，只能判断其是新用户，标记为"1"
                 * 当与服务器完整通信后，应该判断服务器中是否含有该用户的信息，如果没有，那么确认该用户为新用户，并修改该用户的状态为旧用户
                 * 如果服务器中含有该用户的信息，表示该用户不是新用户，状态设为"0"
                 * 如果标记为1，则进行校验
                 */
                if ("1".equals(str)) {
                    //获取状态数据
                    String firstVisitDateValue = firstVisitDateState.value();
                    //判断状态是否为null
                    if (firstVisitDateValue != null) {
                        //更新新老用户标记
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    } else {
                        //经过校验，确认是新用户。设置状态
                        Long ts = jsonObject.getLong("ts");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        firstVisitDateState.update(sdf.format(ts));
                    }
                }
                //返回数据
                return jsonObject;
            }
        });

        //TODO 4 使用侧输出流对原始数据流进行切分

        //页面日志写入主流，启动日志写入侧输出流，曝光数据写入侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageLogDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) throws Exception {

                //获取Start数据
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //启动日志数据
                    ctx.output(startOutputTag, jsonObject.toJSONString());
                } else {
                    //页面日志数据
                    out.collect(jsonObject.toJSONString());
                    //曝光日志数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            //获取单条曝光数据
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            //获取数据中的页面ID
                            String page_id = jsonObject.getJSONObject("page").getString("page_id");
                            //将页面ID插入到曝光数据中
                            displaysJSONObject.put("page_id", page_id);
                            //将曝光数据写入侧输出流
                            ctx.output(displayOutputTag, displaysJSONObject.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 5 将得到的不同的流输出到不同的DWD层主题
        pageLogDS.print("page>>>>>");
        pageLogDS.getSideOutput(startOutputTag).print("start>>>>>");
        pageLogDS.getSideOutput(displayOutputTag).print("display>>>>>");

        pageLogDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        pageLogDS.getSideOutput(startOutputTag).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        pageLogDS.getSideOutput(displayOutputTag).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 6 启动任务
        env.execute();
    }

}
