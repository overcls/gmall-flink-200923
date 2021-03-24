package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * Date:2021/3/23
 * Description:
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");
*/
        //TODO 2 读取3个主题的数据
        String groupId = "visitor_stats_app_0923";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageSource = MyKafkaUtil.getKafkaSource(groupId, pageViewSourceTopic);
        FlinkKafkaConsumer<String> uniqueSource = MyKafkaUtil.getKafkaSource(groupId, uniqueVisitSourceTopic);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(groupId, userJumpDetailSourceTopic);

        DataStreamSource<String> pageSourceDS = env.addSource(pageSource);
        DataStreamSource<String> uniqueSourceDS = env.addSource(uniqueSource);
        DataStreamSource<String> userJumpSourceDS = env.addSource(userJumpSource);
        //TODO 3 取出dwd_page_log主题数据过滤出进入页面数据
        SingleOutputStreamOperator<String> filterPageDS = pageSourceDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        });
        //TODO 4 将4个流统一数据格式
        //将页面流转换格式
        SingleOutputStreamOperator<VisitorStats> pageBeanDS = pageSourceDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    0L,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    System.currentTimeMillis()
            );
        });

        SingleOutputStreamOperator<VisitorStats> uniqueBeanDS = uniqueSourceDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    System.currentTimeMillis()
            );
        });

        SingleOutputStreamOperator<VisitorStats> userJumpBeanDS = userJumpSourceDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    System.currentTimeMillis()
            );
        });

        SingleOutputStreamOperator<VisitorStats> pageEnterBeanDS = filterPageDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    1L,
                    0L,
                    0L,
                    System.currentTimeMillis()
            );
        });
        //TODO 5 union4个流的数据
        DataStream<VisitorStats> unionDS = pageBeanDS.union(uniqueBeanDS, userJumpBeanDS, pageEnterBeanDS);
        //TODO 6 开窗聚合操作
        SingleOutputStreamOperator<VisitorStats> visitorStatsWS = unionDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }));

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsKS = visitorStatsWS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(
                        value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new()
                );
            }
        });

        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> visitorStatsWindowStream = visitorStatsKS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> resultDS = visitorStatsWindowStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        return value1;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        VisitorStats visitorStatsData = iterable.iterator().next();
                        visitorStatsData.setStt(sdf.format(start));
                        visitorStatsData.setEdt(sdf.format(end));
                        collector.collect(visitorStatsData);
                    }
                });
        //TODO 7 将数据写入ClickHouse
        resultDS.print(">>>>>");
        //TODO 8 启动任务
        env.execute();
    }
}