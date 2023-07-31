package org.flinkjob.datastreamtransformation.keypojo;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

class MySumReducer implements ReduceFunction<Player>{

    @Override
    public Player reduce(Player player1, Player player2) throws Exception {
        Integer sum  = player1.score+ player2.score;
        return new Player(player1.name,sum);
    }
}
public class JavaKeyTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        Player p1 = new Player("Kohli",50);
        Player p2 = new Player("Dhoni",75);
        Player p3 = new Player("Rohit",100);
        Player p4 = new Player("Kohli",100);
        Player p5 = new Player("Dhoni",100);
        Player p6 = new Player("Kohli",75);

        DataStream<Player> dataStream =streamExecutionEnvironment.fromElements(p1,p2,p3,p4,p5,p6);

        KeyedStream<Player, Tuple> keyed_ds = dataStream.keyBy("name");
        keyed_ds.reduce(new MySumReducer()).print();

        streamExecutionEnvironment.execute();


    }
}
