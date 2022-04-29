package edu.nwmissouri.s2g2.gangidi;

import java.io.Serializable;

class VotingPage implements Serializable {

    String name = "unknown.md";
    Integer votes;
    Double rank = 1.00;

    public VotingPage(String name, Integer votes) {
        this.name = name;
        this.votes = votes;
    }

    public VotingPage(String name, Double rank, Integer votes) {
        this.name = name;
        this.rank = rank;
        this.votes = votes;

    }

    public String getname() {
        return name;
    }

    public Integer getvotes() {
        return votes;
    }

    public Double getRank() {
        return rank;
    }

    @Override
    public String toString() {
        return "VotingPage [votes=" + votes + ", rank=" + rank + ", name=" + name + "]";
    }

}