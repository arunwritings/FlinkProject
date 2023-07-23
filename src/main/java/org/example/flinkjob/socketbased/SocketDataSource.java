package org.example.flinkjob;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketDataSource {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        DataStream<String> dataStream = streamExecutionEnvironment.socketTextStream("localhost",9000);
        dataStream.print();

        streamExecutionEnvironment.execute();

    }
}
