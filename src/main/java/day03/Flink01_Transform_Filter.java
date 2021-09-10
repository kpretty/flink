package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Project: flink<br/>
 * Package: day03<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/10 09:19<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
public class Flink01_Transform_Filter {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 读取数据并处理
        env.socketTextStream("localhost", 1111)
                .filter(new MyRichFilterFunction())
                .print();

        // 执行任务
        env.execute();
    }

    private static class MyRichFilterFunction extends RichFilterFunction<String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open被调用");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close被调用");
        }

        @Override
        public boolean filter(String value) throws Exception {
            return Double.parseDouble(value) >= 40;
        }
    }
}
