package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_State_Key_ValueState {
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
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Double> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 必须在 open 生命周期初始化
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("state", Double.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Double lastValue = valueState.value();
                        // 第一条数据状态为 null
                        if (lastValue == null || value.getVc() - lastValue <= 10) {
                            out.collect(value.toString());
                        } else {
                            out.collect("异常告警数据" + value);
                        }

                        // 更新状态值
                        valueState.update(value.getVc());

                    }
                })
                .print();

        env.execute();
    }
}
