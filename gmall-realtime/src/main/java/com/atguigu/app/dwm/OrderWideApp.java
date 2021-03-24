package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Date:2021/3/20
 * Description:
 * 数据流：Web/App>>Nginx>>SpringBoot>>MySQL>>FlinkApp>>Kafka(ods)>>FlinkApp>>Kafka(dwd)/Phoenix(dim)>>FlinkApp>>Kafka(dwn)
 * 进程：MockDB>>MySQL>>FlinkCDCApp>>Kafka(ZK)>>BaseDBApp>>Kafka/Phoenix>>OrderWideApp>>Kafka
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*
        //设置CheckPoint，两次CheckPoint之间间隔5000ms
        env.enableCheckpointing(5000L);
        //设置CheckPoint超时时间为10000ms
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //设置CheckPoint的模式为精准一次消费，避免数据丢失和重复
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置CheckPoint的重试策略为3次，每次失败后等待5000ms
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //设置CheckPoint保存路径
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoint"));
        //在IDEA里运行，需要设置登录名才能在HDFS上创建文件夹，避免权限问题
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/
        //TODO 2 读取Kafka dwd_order_info dwd_order_detail主题的数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_app_group";
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(groupId, orderInfoSourceTopic);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(groupId, orderDetailSourceTopic);
        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoSource);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);


        //TODO 3 将2个流转换为JavaBean并提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoWS = orderInfoDS.map(data -> {
            OrderInfo orderInfo = JSON.parseObject(data, OrderInfo.class);

            String create_time = orderInfo.getCreate_time();

            String[] dateTime = create_time.split(" ");

            orderInfo.setCreate_date(dateTime[0]);
            orderInfo.setCreate_hour(dateTime[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWS = orderDetailDS.map(data -> {

            OrderDetail orderDetail = JSON.parseObject(data, OrderDetail.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 4 双流JOIN
        KeyedStream.IntervalJoined<OrderInfo, OrderDetail, Long> joinedDS = orderInfoWS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWS.keyBy(OrderDetail::getOrder_id))
                .inEventTime()
                .between(Time.seconds(-5), Time.seconds(5));

        SingleOutputStreamOperator<OrderWide> orderWideDS = joinedDS.process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
            @Override
            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                out.collect(new OrderWide(orderInfo, orderDetail));
            }
        });

//        orderWideDS.print("OrderWide>>>>>");
        //TODO 5 查询Phoenix,补全维度信息
        //5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            String birthday = dimInfo.getString("BIRTHDAY");
                            Long age = 0L;
                            try {
                                age = (System.currentTimeMillis() - sdf.parse(birthday).getTime()) / 1000 / 60 / 60 / 24 / 365;
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            orderWide.setUser_age(age.intValue());
                            String gender = dimInfo.getString("GENDER");
                            orderWide.setUser_gender(gender);
                        }
                    }
                },
                100,
                TimeUnit.SECONDS);

//        orderWideWithUserDS.print("OrderWideWithUser>>>>>");
        //5.2 关联省份维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            orderWide.setProvince_name(dimInfo.getString("NAME"));
                            orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                            orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                            orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS);

//        orderWideWithProvinceDS.print("OrderWideWithProvince>>>>>");
        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return String.valueOf(input.getSku_id());
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                            orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                            orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                            orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS);
//        orderWideWithSKUDS.print("OrderWideWithSKU>>>>>");
        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSPUDS = AsyncDataStream.unorderedWait(
                orderWideWithSKUDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return String.valueOf(input.getSpu_id());
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS);
//        orderWideWithSPUDS.print("OrderWideWithSPU>>>>>");
        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTMDS = AsyncDataStream.unorderedWait(
                orderWideWithSPUDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide input) {
                        return String.valueOf(input.getTm_id());
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS);

//        orderWideWithTMDS.print("OrderWideWithTM>>>>>");
        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTMDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide input) {
                        return String.valueOf(input.getCategory3_id());
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            orderWide.setCategory3_name(dimInfo.getString("NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS);

//        orderWideWithCategory3DS.print("OrderWideWithCategory3>>>>>");
        //TODO 6 将数据写入Kafka
        orderWideWithCategory3DS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        //TODO 7 启动任务
        env.execute();
    }
}
