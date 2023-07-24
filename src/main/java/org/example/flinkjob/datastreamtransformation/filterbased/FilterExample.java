package org.example.flinkjob.datastreamtransformation.filterbased;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

class MyFilterFunction implements FilterFunction<Integer>{

    @Override
    public boolean filter(Integer integer) throws Exception {
        if (integer>10)return true;
        else return false;
    }
}
public class FilterExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        DataStream<Integer> dataStream = streamExecutionEnvironment.fromElements(12,32,2,4,23,9);
        dataStream.filter(new MyFilterFunction()).print();

        streamExecutionEnvironment.execute();
    }
}
