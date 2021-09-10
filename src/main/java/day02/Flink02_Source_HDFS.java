package day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Project: flink<br/>
 * Package: day02<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/08 18:07<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
public class Flink02_Source_HDFS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        env.readTextFile("hdfs://localhost:8020/平台密码.txt").print();

        env.execute();
    }
}
