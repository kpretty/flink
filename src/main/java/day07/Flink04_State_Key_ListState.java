package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink04_State_Key_ListState {
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
                    private ListState<Double> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Double>("", Double.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, List<Double>>.Context ctx, Collector<List<Double>> out) throws Exception {
                        // 直接添加
                        listState.add(value.getVc());
                        // 若超过三个 剔除最小的一个
                        ArrayList<Double> list = new ArrayList<>();
                        for (Double el : listState.get()) {
                            list.add(el);
                        }
                        if (list.size() > 3) {
                            // 排序
                            list.sort((o1, o2) -> Double.compare(o2, o1));
                            list.remove(3);
                        }
                        // 更新状态值，覆盖全部元素
                        listState.update(list);
                        out.collect(list);
                    }
                })
                .print();

        env.execute();
    }
}
