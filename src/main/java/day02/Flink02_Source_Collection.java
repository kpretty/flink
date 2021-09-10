package day02;

import bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * Project: flink<br/>
 * Package: day02<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/08 18:03<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));

        env.fromCollection(waterSensors).print();

        env.execute();
    }
}
