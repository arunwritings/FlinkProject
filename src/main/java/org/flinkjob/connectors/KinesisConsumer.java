package org.flinkjob.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class KinesisConsumer {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.put(AWSConfigConstants.AWS_REGION, "us-east-1");
        prop.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "AADWkjnekjNJIAFGKkafee316fe");
        prop.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "WR3INkejnfkAJFFlfqfke315fkrmbkd");
        prop.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        DataStream<String> ds = env.addSource(new FlinkKinesisConsumer<>("flink", new SimpleStringSchema(), prop));
        ds.print();
        env.execute();
    }
}
