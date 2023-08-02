package org.flinkjob.window;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.ThreadLocalRandom;

class CarSpeed{
    public Integer carId;
    public Integer carSpeed;
    public CarSpeed(Integer carId, Integer carSpeed){
        this.carId=carId;
        this.carSpeed=carSpeed;
    }
}
class CarTemperature{
    public Integer carId;
    public Integer carTemperature;
    public CarTemperature(Integer carId, Integer carTemperature){
        this.carId=carId;
        this.carTemperature=carTemperature;
    }
}
class CarSpeedSource extends RichSourceFunction<CarSpeed>{
    @Override
    public void run(SourceContext<CarSpeed> sourceContext) throws Exception {
        while (true){
            int i=0;
            for (i=0;i<10;i++){
                Integer randomCar = ThreadLocalRandom.current().nextInt(1,9+1);
                Integer randomSpeed = ThreadLocalRandom.current().nextInt(50,100+1);
                sourceContext.collect(new CarSpeed(randomCar, randomSpeed));
            }
            Thread.sleep(2000);
        }
    }
    @Override
    public void cancel() {
    }
}
class CarTemperatureSource extends RichSourceFunction<CarTemperature>{

    @Override
    public void run(SourceContext<CarTemperature> sourceContext) throws Exception {
        while (true){
            int i=0;
            for (i=0;i<10;i++){
                Integer randomCar = ThreadLocalRandom.current().nextInt(1,9+1);
                Integer randomTemperature = ThreadLocalRandom.current().nextInt(50,100+1);
                sourceContext.collect(new CarTemperature(randomCar, randomTemperature));
            }
            Thread.sleep(2000);
        }
    }
    @Override
    public void cancel() {
    }
}
public class WindowJoinExample {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        DataStream<CarSpeed> carSpeedDataStream = environment.addSource(new CarSpeedSource());
        DataStream<CarTemperature> carTemperatureDataStream = environment.addSource(new CarTemperatureSource());

        carSpeedDataStream.join(carTemperatureDataStream).where(new KeySelector<CarSpeed, Integer>() {
            @Override
            public Integer getKey(CarSpeed carSpeed) throws Exception {
                return carSpeed.carId;
            }
        }).equalTo(new KeySelector<CarTemperature, Integer>() {
            @Override
            public Integer getKey(CarTemperature carTemperature) throws Exception {
                return carTemperature.carId;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).apply(new JoinFunction<CarSpeed, CarTemperature, String>() {
            @Override
            public String join(CarSpeed carSpeed, CarTemperature carTemperature) throws Exception {
                return "Car Id: "+carSpeed.carId+", Car Speed: "+carSpeed.carSpeed+", Car Temperature: "+carTemperature.carTemperature;
            }
        }).print();

        environment.execute();
    }
}
