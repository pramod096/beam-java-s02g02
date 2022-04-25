package edu.nwmissouri.s2g2.pramod;

import java.io.Serializable;
import java.util.ArrayList;

class RankedPage implements Serializable {
    private String rankedPageName;
    private Double rankValue;
    private ArrayList<VotingPage> pagesVoted;


// Constructors
       public RankedPage(String rankedPageName, ArrayList<VotingPage> pagesVoted)
        {
        this.rankedPageName = name;
        this.pagesVoted = pagesVoted;

    }

    public RankedPage(String rankedPageName, Double rankValue, ArrayList<VotingPage> pagesVoted) {
        this.rankedPageName = rankedPageName;
        this.rankValue = rankValue;
        this.pagesVoted = pagesVoted;

    }

// Getters and Setters
    public String getRankedPageName() {
        return rankedPageName;
    }

    public void setRankedPagename(String rankedPageName) {
        this.rankedPageName = rankedPageName;
    }

    public Double getRankValue() {
        return rankValue;
    }

    public void setRank(Double rankValue) {
        this.rankValue = rankValue;
    }

    public ArrayList<VotingPage> getPagesVoted() {
        return pagesVoted;
    }

    public void setVoters(ArrayList<VotingPage> pagesVoted) {
        this.pagesVoted = pagesVoted;
    }

        @Override
    public String toString() {
        return String.format("%s %.6f %s", rankedPageName, rankValue, pagesVoted);
    }
}
