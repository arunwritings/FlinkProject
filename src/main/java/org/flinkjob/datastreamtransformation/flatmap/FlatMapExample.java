package org.flinkjob.datastreamtransformation.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

class MyFlatMapFunction  implements FlatMapFunction<String,String> {

    @Override
    public void flatMap(String arg0, Collector<String> arg1) throws Exception {
        String[] arr = arg0.split(",");
        for (String input : arr){
            arg1.collect(input);
        }
    }
}
public class FlatMapExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        DataStream<String> dataStream = streamExecutionEnvironment.fromElements("one,two","three","four,five");
        dataStream.flatMap(new MyFlatMapFunction()).print();

        streamExecutionEnvironment.execute();
    }
}
