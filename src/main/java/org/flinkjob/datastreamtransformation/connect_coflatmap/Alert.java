package org.flinkjob.datastreamtransformation.connect_coflatmap;

public class Alert {

    public String message;

    public double timeStamp;

    public Alert(){
    }

    public Alert(String message, double timeStamp){
        this.message=message;
        this.timeStamp=timeStamp;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
