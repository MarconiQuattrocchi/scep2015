package it.polimi.scep15.stream.events;
import java.util.List;


public class RankEvent {
	
	List<String> rank;
	List<Long> counts;
	private List<String> pickupAreaCodes;

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
	
	public List<String> getPickupAreaCodes() {
		return pickupAreaCodes;
	}

	public void setPickupAreaCodes(List<String> pickupAreaCodes) {
		this.pickupAreaCodes = pickupAreaCodes;
	}

	
	public String toString(){
		return "{"+rank+","+counts+"}";
	}
}
