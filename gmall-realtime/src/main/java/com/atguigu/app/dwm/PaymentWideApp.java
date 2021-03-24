package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * Date:2021/3/22
 * Description:
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        //TODO 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*
        //设置Checkpoint，两次CheckPoint之间的间隔时间
        env.enableCheckpointing(5000L);
        //设置CheckPoint的模式为精准一次消费
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置CheckPoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //设置CheckPoint的重试策略：3次，每次失败后间隔5000ms再次重试
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //设置CheckPoint的保存路径
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint"));
        //在IDEA里运行，需要设置登录名才能在HDFS上创建文件夹，避免权限问题
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/
        //TODO 2、读取dwd_payment_info和dwm_order_wide主题的数据
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(groupId, paymentInfoSourceTopic);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(groupId, orderWideSourceTopic);
        DataStreamSource<String> paymentInfoDS = env.addSource(paymentInfoSource);
        DataStreamSource<String> orderWideDS = env.addSource(orderWideSource);

//        paymentInfoDS.print("PaymentInfoDS>>>>>");
//        orderWideDS.print("OrderWideDS>>>>>");
        //TODO 3、将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWS = paymentInfoDS.map(data -> JSON.parseObject(data, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！");
                                }
                            }
                        }));
        SingleOutputStreamOperator<OrderWide> orderWideWS = orderWideDS.map(data -> JSON.parseObject(data, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！");
                                }
                            }
                        }));
//        paymentInfoWS.print("PaymentInfoWS>>>>>");
//        orderWideWS.print("OrderWideWS>>>>>");
        //TODO 4、双流JOIN
        SingleOutputStreamOperator<PaymentWide> joinedDS = paymentInfoWS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideWS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.minutes(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
//        joinedDS.print("JoinedDS>>>>>");
        //TODO 5、将数据写入Kafka的dwm_payment_wide主题
        joinedDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));
        //TODO 6、启动任务
        env.execute();
   }
}
