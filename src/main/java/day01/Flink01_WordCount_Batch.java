package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Project: flink<br/>
 * Package: day01<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/03 16:48<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据
        DataSource<String> input = env.readTextFile("input");

        // 3.扁平化(新建类)
        FlatMapOperator<String, String> wordDS = input.flatMap(new WordCountFlatMap());

        // 4.将单词转换为元组(匿名内部类)
        MapOperator<String, Tuple2<String, Integer>> map = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 5.分组，索引从0，字段名适用样例类和java bean
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = map.groupBy(0);

        // 6.聚合
        AggregateOperator<Tuple2<String, Integer>> out = groupBy.sum(1);

        // 7.打印
        out.print();
    }

    // 自定义实现扁平化操作的类
    public static class WordCountFlatMap implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            // 按照空格切割
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }

        }
    }
}
