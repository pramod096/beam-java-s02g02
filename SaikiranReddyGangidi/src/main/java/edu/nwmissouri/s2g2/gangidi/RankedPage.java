package edu.nwmissouri.s2g2.gangidi;

import java.io.Serializable;
import java.util.ArrayList;

class RankedPage implements Serializable {
    String name;
    Double rank;
    Integer vote;

    public RankedPage(String name, Double rank, Integer vote) {
        this.name = name;
        this.rank = rank;
        this.vote = vote;
    }

    public RankedPage(String name, Double rank) {
        this.name = name;
        this.rank = rank;
    }

    public RankedPage(String name, Integer vote) {
        this.name = name;
        this.vote = vote;
    }

    public RankedPage(String key, ArrayList<VotingPage> voters) {
    }
}