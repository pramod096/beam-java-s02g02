package edu.nwmissouri.s2g2.gangidi;

import java.io.Serializable;

class VotingPage implements Serializable {

    String votername = "unknown.md";
    Integer contributorVotes = 0;
    Double rank = 1.00;

    public VotingPage(String voterName, Integer contributorVotes) {
        this.votername = voterName;
        this.contributorVotes = contributorVotes;
    }

    public VotingPage(String votername, Double rank, Integer contributorVotes) {
        this.votername = votername;
        this.rank = rank;
        this.contributorVotes = contributorVotes;

    }

    public String getVotername() {
        return votername;
    }

    public void setVotername(String votername) {
        this.votername = votername;
    }

    public Integer getContributorVotes() {
        return contributorVotes;
    }

    public void setContributorVotes(Integer contributorVotes) {
        this.contributorVotes = contributorVotes;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return String.format("%s,%.5f,%d",votername,rank,contributorVotes);
    }

}