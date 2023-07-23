package org.example.flinkjob;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

class StringSource extends RichParallelSourceFunction<String>{

    @Override
    public void run(SourceContext<String> arg0) throws Exception {
        Integer integer=0;
        while (true){
            arg0.collect(""+integer);
            integer=integer+1;
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
    }
}

public class StreamingSinkFileExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();
        streamExecutionEnvironment.enableCheckpointing(1000);
        streamExecutionEnvironment.setStateBackend((StateBackend) new FsStateBackend("file:///C:/checkpoint"));

        DataStream<String> dataStream = streamExecutionEnvironment.addSource(new StringSource());

        StreamingFileSink<String> stringStreamingFileSink = StreamingFileSink.forRowFormat(new Path("C:\\ETL"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH")).build();

        dataStream.addSink(stringStreamingFileSink);
        streamExecutionEnvironment.execute();

    }
}
