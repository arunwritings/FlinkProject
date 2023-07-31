package org.flinkjob.filebased;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.concurrent.TimeUnit;

public class FileBasedExample {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();
        String path = "C:\\Flink\\input";
        TextInputFormat textInputFormat = new TextInputFormat(new Path(path));

       /* DataStream<String> dataStream = streamExecutionEnvironment.readFile(textInputFormat,path);
        dataStream.print();*/

        DataStream<String> ds = streamExecutionEnvironment.readFile(textInputFormat,path, FileProcessingMode.PROCESS_CONTINUOUSLY,1000, BasicTypeInfo.STRING_TYPE_INFO);
        ds.print();
        streamExecutionEnvironment.execute();
    }
}
