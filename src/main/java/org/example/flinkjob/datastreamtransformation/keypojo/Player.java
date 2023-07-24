package org.example.flinkjob.datastreamtransformation.keypojo;

public class Player {

    public String name;
    public Integer score;

    public Player(){

    }

    public Player(String name, Integer score){
        this.name=name;
        this.score=score;
    }
    @Override
    public String toString() {
        return "Player{" +
                "name='" + name + '\'' +
                ", score=" + score +
                '}';
    }
}
