package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_Max {
    /*
    需求：
    1. 从端口接收水位传感器数据
    2. 输出每个传感器最高的数据
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
                .maxBy("vc",false)
                .print();

        // 执行任务
        env.execute();
    }
}
