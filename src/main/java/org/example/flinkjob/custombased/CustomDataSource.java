package org.example.flinkjob.custombased;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flinkjob.custombased.HeapMetrics;
import org.example.flinkjob.custombased.HeapMonitorSource;

public class CustomDataSource {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        DataStream<HeapMetrics> dataStream = streamExecutionEnvironment.addSource(new HeapMonitorSource());
        dataStream.print();

        streamExecutionEnvironment.execute();
    }
}
