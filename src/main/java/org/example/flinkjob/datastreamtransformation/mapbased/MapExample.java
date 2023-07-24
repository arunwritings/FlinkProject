package org.example.flinkjob.datastreamtransformation.mapbased;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
class MyMapFunction implements MapFunction<String,String>{

    @Override
    public String map(String arg0) throws Exception {
        return arg0.toUpperCase();
    }
}
public class MapExample {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        DataStream<String> dataStream = streamExecutionEnvironment.fromElements("one","two","three");

        DataStream<String> stringDataStream = dataStream.map(new MyMapFunction());
        stringDataStream.print();

        streamExecutionEnvironment.execute();

    }
}
