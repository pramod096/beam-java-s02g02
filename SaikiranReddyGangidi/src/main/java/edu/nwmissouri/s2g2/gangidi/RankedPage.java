package edu.nwmissouri.s2g2.gangidi;

import java.io.Serializable;
import java.util.ArrayList;

class RankedPage implements Serializable {
    String name = "UNKONWN";
    Double rank = 1.000;
    ArrayList<VotingPage> voters = null;

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

    public ArrayList<VotingPage> getVoters() {
        return voters;
    }

    public void setVoters(ArrayList<VotingPage> voters) {
        this.voters = voters;
    }

    @Override
    public String toString() {
        return String.format("%s %.5f %s", name, rank, voters);
    }

    public RankedPage(String name, ArrayList<VotingPage> voters) {
        this.name = name;
        this.voters = voters;

    }

    public RankedPage(String name, Double rank, ArrayList<VotingPage> voters) {
        this.name = name;
        this.rank = rank;
        this.voters = voters;

    }
}