package org.flinkjob.streamingETL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.Serializable;
import java.util.Properties;

class User implements Serializable {
    private String uid;
    private String uname;
    public User(){
    }
    public User(String id,String name) {
        this.uid=id;
        this.uname=name;
    }
    @Override
    public String toString() {
        return this.uid+","+this.uname;
    }
}
class ConvertStringtoUser implements MapFunction<String, User> {
    @Override
    public User map(String arg0) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tweetJson = mapper.readTree(arg0);
        String uid = tweetJson.get("uid").textValue();
        String username= tweetJson.get("uname").textValue();
        return new User(uid,username);
    }
}

public class KafkaStreamingETL {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///C:/checkpoint"));
        Properties perp = new Properties();
        perp.setProperty("bootstrap.servers", "localhost:9092");
        perp.setProperty("group.id", "flinkreadkafka");
        DataStream<String> ds = env.addSource(new FlinkKafkaConsumer("tinput", new SimpleStringSchema(), perp));
        ds.print();
        DataStream<User> us = ds.map(new ConvertStringtoUser());
        SinkFunction<User> sink = StreamingFileSink.forRowFormat(new Path("C://ETL"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd--HH")).build();
        us.addSink(sink);
        env.execute();
    }

    }
