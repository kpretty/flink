package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Reduce {
    /*
    需求：
    1. 从端口接收水位传感器数据
    2. 输出最新的时间和最大的温度
     */
    public static void main(String[] args) throws Exception {
// 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 端口接收数据
        env.socketTextStream("localhost", 1111)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] els = value.split(" ");
                    return new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2]));
                }).returns(Types.POJO(WaterSensor.class))
                .keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value2.getTs(), Double.max(value1.getVc(), value2.getVc()));
                    }
                })
                .print();

        // 执行任务
        env.execute();
    }
}
