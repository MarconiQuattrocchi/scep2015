package it.polimi.scep15.test.events;
import java.util.List;


public class RankEvent {
	
	List<String> rank;
	List<Long> counts;

	public List<String> getRank() {
		return rank;
	}

	public void setRank(List<String> rank) {
		this.rank = rank;
	}

	public List<Long> getCounts() {
		return counts;
	}

	public void setCounts(List<Long> counts) {
		this.counts = counts;
	}
	
	public String toString(){
		return "{"+rank+","+counts+"}";
	}
}
