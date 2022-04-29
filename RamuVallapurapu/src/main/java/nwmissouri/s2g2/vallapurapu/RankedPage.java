package nwmissouri.s2g2.vallapurapu;

import java.util.ArrayList;

public class RankedPage {

	private String rankedPageName = "";
	private Double rankValue = 1.0;
	private ArrayList<VotingPage> pagesVoted;

	public RankedPage(String rankedPageName, Double rankValue, ArrayList<VotingPage> pagesVoted) {
		super();
		this.rankedPageName = rankedPageName;
		this.rankValue = rankValue;
		this.pagesVoted = pagesVoted;
	}

	public RankedPage(String rankedPageName, ArrayList<VotingPage> pagesVoted) {
		super();
		this.rankedPageName = rankedPageName;
		this.pagesVoted = pagesVoted;
	}

	public String getRankedPageName() {
		return rankedPageName;
	}

	public void setRankedPageName(String rankedPageName) {
		this.rankedPageName = rankedPageName;
	}

	public Double getRankValue() {
		return rankValue;
	}

	public void setRankValue(Double rankValue) {
		this.rankValue = rankValue;
	}

	public ArrayList<VotingPage> getPagesVoted() {
		return pagesVoted;
	}

	public void setPagesVoted(ArrayList<VotingPage> pagesVoted) {
		this.pagesVoted = pagesVoted;
	}

	@Override
	public String toString() {
		return String.format("%s %.4f %s", rankedPageName, rankValue, pagesVoted);
	}

}