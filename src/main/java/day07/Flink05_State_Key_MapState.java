package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink05_State_Key_MapState {
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
                .process(new KeyedProcessFunction<String, WaterSensor, List<Double>>() {
                    private MapState<Double, Integer> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Double, Integer>("state", Double.class, Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, List<Double>>.Context ctx, Collector<List<Double>> out) throws Exception {
                        // 直接插入
                        mapState.put(value.getVc(), 1);
                        ArrayList<Double> list = new ArrayList<>();
                        for (Double key : mapState.keys()) {
                            list.add(key);
                        }
                        out.collect(list);
                    }
                })
                .print();

        env.execute();
    }
}
