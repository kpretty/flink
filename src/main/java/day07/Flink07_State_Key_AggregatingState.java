package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_State_Key_AggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 1111)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] els = value.split(" ");
                        return new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2]));
                    }
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Double>() {
                    private AggregatingState<WaterSensor, Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState =
                                getRuntimeContext()
                                        .getAggregatingState(
                                                new AggregatingStateDescriptor<WaterSensor, Tuple2<Integer, Double>, Double>(
                                                        "state",
                                                        new MyAggFunction(),
                                                        Types.TUPLE(Types.INT, Types.DOUBLE)));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, Double>.Context ctx, Collector<Double> out) throws Exception {
                        aggregatingState.add(value);
                        out.collect(aggregatingState.get());
                    }

                })
                .print();

        env.execute();
    }

    private static class MyAggFunction implements AggregateFunction<WaterSensor, Tuple2<Integer, Double>, Double> {

        @Override
        public Tuple2<Integer, Double> createAccumulator() {
            return Tuple2.of(0, 0D);
        }

        @Override
        public Tuple2<Integer, Double> add(WaterSensor value, Tuple2<Integer, Double> accumulator) {
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value.getVc());
        }

        @Override
        public Double getResult(Tuple2<Integer, Double> accumulator) {
            return accumulator.f1 / accumulator.f0;
        }

        @Override
        public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
