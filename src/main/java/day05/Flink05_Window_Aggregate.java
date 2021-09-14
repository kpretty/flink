package day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Flink05_Window_Aggregate {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据并处理
        env.socketTextStream("localhost", 1111)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                // 开窗
                .window(TumblingProcessingTimeWindows.of(Time.seconds(6L)))
                .aggregate(new MyAggregate(), new MyWindowFunction())
                .print();

        // 执行任务
        env.execute();
    }

    private static class MyAggregate implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {


        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }


    private static class MyWindowFunction implements WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("当前窗口的 key：" + key);
            System.out.println("当前窗口是：[ " + new Date(window.getStart()) + ", " + new Date(window.getEnd()) + " )");
            // input 迭代器一定是只有一个元素
            out.collect(Tuple2.of(key, input.iterator().next()));
        }
    }
}
