package day03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_Transform_Process {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据并尽可能通过 process 处理数据
        env.socketTextStream("localhost", 1111)
                .process(new FlatMapProcess())
                .process(new MapProcess())
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        // 执行任务
        env.execute();
    }

    // 使用 process 模拟 flatMap
    private static class FlatMapProcess extends ProcessFunction<String, String> {

        @Override
        public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
            String[] els = value.split(" ");
            for (String el : els) {
                out.collect(el);
            }
        }
    }

    // 使用 process 模拟 flatMap
    private static class MapProcess extends ProcessFunction<String, Tuple2<String, Integer>> {

        @Override
        public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(Tuple2.of(value, 1));
        }
    }
}
