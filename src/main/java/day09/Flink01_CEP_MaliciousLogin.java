package day09;

import bean.UserActionLogin;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class Flink01_CEP_MaliciousLogin {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取文件数据
        DataStreamSource<String> fileDS = env.readTextFile("input/LoginLog.csv");
        // 提取事件时间
        WatermarkStrategy<String> wm = WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[3]) * 1000;
            }
        });

        // 处理用户行为数据
        KeyedStream<UserActionLogin, String> userActionDS = fileDS.assignTimestampsAndWatermarks(wm)
                .map(new MapFunction<String, UserActionLogin>() {
                    @Override
                    public UserActionLogin map(String value) throws Exception {
                        String[] infos = value.split(",");
                        return new UserActionLogin(infos[0], infos[1], infos[2], Long.parseLong(infos[3]));
                    }
                })
                .keyBy(UserActionLogin::getId);

        // 创建模式
        Pattern<UserActionLogin, UserActionLogin> pattern = Pattern.<UserActionLogin>begin("start").where(new SimpleCondition<UserActionLogin>() {
            @Override
            public boolean filter(UserActionLogin value) throws Exception {
                return "fail".equals(value.getStatus());
            }
        }).next("next").where(new SimpleCondition<UserActionLogin>() {
            @Override
            public boolean filter(UserActionLogin value) throws Exception {
                return "fail".equals(value.getStatus());
            }
        }).within(Time.seconds(5));

        // 应用模式到流
        PatternStream<UserActionLogin> patternDS = CEP.pattern(userActionDS, pattern);

        // 事件提取
        SingleOutputStreamOperator<String> select = patternDS.select(new PatternSelectFunction<UserActionLogin, String>() {
            @Override
            public String select(Map<String, List<UserActionLogin>> pattern) throws Exception {
                // 若模式不是循环模式则List只会有一个元素
                // Map的 key 为模式名称如：start，next
                List<UserActionLogin> start = pattern.get("start");
                List<UserActionLogin> next = pattern.get("next");
                return "用户 " + start.get(0).getId() + " 在 " + start.get(0).getTs() + " ~ " + next.get(0).getTs() + " 间，恶意登录两次";
            }
        });

        // 打印
        select.print();

        // 任务执行
        env.execute();
    }
}
