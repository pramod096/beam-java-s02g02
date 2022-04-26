package edu.nwmissouri.s2g2.pramod;

import java.io.Serializable;

import org.apache.beam.sdk.options.Default.Integer;

class VotingPage implements Serializable {
    private String name;
    private Double rank;
    private Integer votes;

    public VotingPage(String name, Integer votes) {
        this.name = name;
        this.votes = votes;

    }

    public VotingPage(String name, Double rank, Integer votes) {
        this.name = name;
        this.rank = rank;
        this.votes = votes;

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    public Integer getVotes() {
        return votes;
    }

    public void setVotes(Integer votes) {
        this.votes = votes;
    }

    @Override
    public String toString() {
        return String.format("%s %.6f %s", name, rank, votes);
    }
}
