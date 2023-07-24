package org.example.flinkjob.datastreamtransformation.connect_coflatmap;

public class SensorReading {

    public String id;

    public double temperature;

    public SensorReading(){

    }

    public SensorReading(String id, double temperature){
        this.id=id;
        this.temperature=temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}
