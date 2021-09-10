package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 1111)
                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(x -> Tuple2.of(x, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
