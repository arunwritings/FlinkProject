package org.flinkjob.connectors;

import org.apache.flink.kinesis.shaded.com.amazonaws.auth.AWSStaticCredentialsProvider;
import org.apache.flink.kinesis.shaded.com.amazonaws.auth.BasicAWSCredentials;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.AmazonKinesis;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.PutRecordRequest;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.PutRecordResult;

import java.nio.ByteBuffer;

public class KinesisProducer {
    public static void main(String[] args) {
        BasicAWSCredentials cred = new BasicAWSCredentials("AADWkjnekjNJIAFGKkafee316fe", "WR3INkejnfkAJFFlfqfke315fkrmbkd");
        AmazonKinesis client = AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(cred)).withRegion("us-east-1").build();
        PutRecordRequest request = new PutRecordRequest();
        request.setStreamName("flink");
        request.setPartitionKey("1");
        request.setData(ByteBuffer.wrap("hello world".getBytes()));
        PutRecordResult result = client.putRecord(request);
        System.out.println(result.getShardId());
    }
}
