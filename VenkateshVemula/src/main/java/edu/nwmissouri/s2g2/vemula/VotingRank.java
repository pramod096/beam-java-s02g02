package edu.nwmissouri.s2g2.vemula;

import java.io.Serializable;
import java.util.ArrayList;

class RankedPage implements Serializable {
    String name = "UNKONWN";
    Double rank = 1.000;
    ArrayList<VotingPage> voters = null;

    public String getName() {
        return name;
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


    public Double getRank() {
        return rank;
    }

   
    public ArrayList<VotingPage> getVoters() {
        return voters;
    }

   
    @Override
    public String toString() {
        return String.format("%s %.5f %s", name, rank, voters);
    }

  
}