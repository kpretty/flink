package day09;

import bean.UserActionLogin;
import bean.UserActionOrder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class Flink03_CEP_TimeoutOrder {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<String> fileDS = env.readTextFile("input/OrderLog.csv");

        // 提取事件时间
        WatermarkStrategy<String> wm = WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[3]) * 1000;
            }
        });

        // 处理数据
        KeyedStream<UserActionOrder, String> userActionDS = fileDS.assignTimestampsAndWatermarks(wm)
                .map(new MapFunction<String, UserActionOrder>() {
                    @Override
                    public UserActionOrder map(String value) throws Exception {
                        String[] infos = value.split(",");
                        return new UserActionOrder(infos[0], infos[1], infos[2], Long.parseLong(infos[3]));
                    }
                })
                .keyBy(UserActionOrder::getId);

        // 定义模式
        Pattern<UserActionOrder, UserActionOrder> pattern = Pattern.<UserActionOrder>begin("start")
                .where(new SimpleCondition<UserActionOrder>() {
                    @Override
                    public boolean filter(UserActionOrder value) throws Exception {
                        return "create".equals(value.getAction());
                    }
                })
                .followedBy("follow").where(new SimpleCondition<UserActionOrder>() {
                    @Override
                    public boolean filter(UserActionOrder value) throws Exception {
                        return "pay".equals(value.getAction());
                    }
                })
                .within(Time.minutes(15));

        // 将模式应用到流上
        PatternStream<UserActionOrder> patternDS = CEP.pattern(userActionDS, pattern);

        // 侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("超时订单") {
        };

        // 提取事件
        SingleOutputStreamOperator<String> result = patternDS.select(outputTag, new MyPatternTimeoutFunction(), new MyPatternSelectFunction());

        // 输出
        result.print("正常订单");
        result.getSideOutput(outputTag).print("超时订单");

        // 任务执行
        env.execute();
    }

    private static class MyPatternTimeoutFunction implements PatternTimeoutFunction<UserActionOrder, String> {

        @Override
        public String timeout(Map<String, List<UserActionOrder>> pattern, long timeoutTimestamp) throws Exception {
            // 超时事件一定在 create 中，若出现在 follow 则就是正常支付的
            UserActionOrder order = pattern.get("start").get(0);
            return "用户 " + order.getId() + " 在 " + order.getTs() + "创建订单，在 " + timeoutTimestamp / 1000L + " 支付订单，超时";
        }
    }

    private static class MyPatternSelectFunction implements PatternSelectFunction<UserActionOrder, String> {

        @Override
        public String select(Map<String, List<UserActionOrder>> pattern) throws Exception {
            UserActionOrder create = pattern.get("start").get(0);
            UserActionOrder pay = pattern.get("follow").get(0);
            return "用户 " + create.getId() + " 在 " + create.getTs() + "创建订单，在 " + pay.getTs() + " 支付订单，正常";
        }
    }
}
