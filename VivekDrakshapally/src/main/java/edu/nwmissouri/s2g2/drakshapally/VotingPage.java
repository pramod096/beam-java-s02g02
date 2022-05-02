package edu.nwmissouri.s2g2.drakshapally;

import java.io.Serializable;

class VotingPage implements Serializable {

    private String name = "";
    private Double rank = 1.0;
    private Integer votes = 0;

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
        return String.format("%s %.4f %s", name, rank, votes);
    }
}