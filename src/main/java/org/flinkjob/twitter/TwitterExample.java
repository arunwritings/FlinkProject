package org.flinkjob.twitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

class GetTopHashTag implements AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {
    @Override
    public void apply(TimeWindow arg0, Iterable<Tuple2<String, Integer>> arg1, Collector<String> arg2) throws Exception {
        Tuple2<String, Integer> topHashTag = new Tuple2("", 0);
        for (Tuple2<String, Integer> tuple2 : arg1) {
            if (tuple2.f1 > topHashTag.f1) {
                topHashTag = tuple2;
            }
        }
        arg2.collect("HashTag::" + topHashTag.f0 + "count::" + topHashTag.f1);
    }
}

class FilterHashTag implements FilterFunction<Tuple2<String, Integer>> {
    @Override
    public boolean filter(Tuple2<String, Integer> arg0) throws Exception {
        return arg0.f1 > 1;
    }
}

class ExtractHashTags implements FlatMapFunction<String, Tuple2<String, Integer>> {
    public void flatMap(String arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tweetJson = mapper.readTree(arg0);

        JsonNode entityJson = tweetJson.get("entities");

        if (entityJson == null) return;

        JsonNode hashtags = entityJson.get("hashtags");
        if (hashtags == null) return;

        if (hashtags.isArray()) {

            ArrayNode arrayNode = (ArrayNode) hashtags;

            for (int i = 0; i < arrayNode.size(); i++) {
                String hs = arrayNode.get(i).get("text").textValue();

                if (hs.matches("\\w+")) {
                    arg1.collect(new Tuple2(hs, 1));
                }
            }
        }
    }
}

public class TwitterExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        Properties properties = new Properties();
        properties.setProperty(TwitterSource.CONSUMER_KEY, "YrAkFEv3xryXHhWBOrrqtXakM");
        properties.setProperty(TwitterSource.CONSUMER_SECRET, "lilU7zIiJyCTw09k0jNZ3qrBWdP8UqUEciv5GEDuRJdwLVmaRT");
        properties.setProperty(TwitterSource.TOKEN, "836587835977183234-P8xonLRtXh4ZNqfohElnmWdTAQ4I35E");
        properties.setProperty(TwitterSource.TOKEN_SECRET, "GkJh0dEDaXB2HEdNc5FTzYjaHXYkJLhYPsi9BpUWN731n");

        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> ds = environment.addSource(new TwitterSource(properties));
        ds.print();

        ds.flatMap(new ExtractHashTags()).
                keyBy(0).timeWindow(Time.seconds(30)).sum(1).
                filter(new FilterHashTag()).
                timeWindowAll(Time.seconds(30)).
                apply(new GetTopHashTag()).
                print();

        environment.execute();
    }
}
