package day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink02_Source_HDFS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        env.readTextFile("hdfs://localhost:8020/平台密码.txt").print();

        env.execute();
    }
}
