package org.flinkjob.state_stateful;

public class Weather {

    public String city;

    public Integer temperature;

    public Weather(){
    }
    public Weather(String city, Integer temperature){
        this.city=city;
        this.temperature=temperature;
    }

    @Override
    public String toString() {
        return "Weather{" +
                "city='" + city + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}
