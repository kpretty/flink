package day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;
import java.util.Set;

/**
 * Project: flink<br/>
 * Package: day03<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/10 09:36<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
public class Flink03_Transform_Distinct {
    // 接收端口数据，进行单词按空格拆分，并进行去重输出（即每个单词只会输出一次）
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        //env.setParallelism(1);

        // 接收端口数据并处理
        env.socketTextStream("localhost", 1111)
                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .keyBy(x -> x)
                .filter(new MyRichFilterFunction())
                .print();

        // 执行任务
        env.execute();
    }

    private static class MyRichFilterFunction extends RichFilterFunction<String> {

        private Set<String> wordSet;

        @Override
        public void open(Configuration parameters) throws Exception {
            wordSet = new HashSet<>();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public boolean filter(String value) throws Exception {
            if (wordSet.contains(value)) {
                return false;
            } else {
                wordSet.add(value);
                return true;
            }
        }
    }
}
