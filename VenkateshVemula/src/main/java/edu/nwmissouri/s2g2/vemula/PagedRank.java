package edu.nwmissouri.s2g2.vemula;

import java.io.Serializable;

class VotingPage implements Serializable {

    String name = "unknown.md";
    Integer vote;
    Double rank = 1.00;

    public VotingPage(String name, Integer vote) {
        this.name = name;
        this.vote = vote;
    }

    public VotingPage(String name, Double rank, Integer vote) {
        this.name = name;
        this.rank = rank;
        this.vote = vote;

    }

    public String getname() {
        return name;
    }

    public Integer getvote() {
        return vote;
    }

    
    public Double getRank() {
        return rank;
    }

    
    @Override
    public String toString() {
        return String.format("%s, %.5f, %d",name,rank,vote);
    }

}