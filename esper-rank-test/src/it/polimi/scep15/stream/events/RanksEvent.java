package it.polimi.scep15.stream.events;
import java.util.List;


public class RanksEvent {
	
	private List<String> prevRank;
	private List<String> currentRank;
	private List<String> pickupAreaCodes;
	private List<Long> currentCounts;

	
	public List<String> getCurrentRank() {
		return currentRank;
	}
	public void setCurrentRank(List<String> currentRank) {
		this.currentRank = currentRank;
	}
	
	public List<String> getPrevRank() {
		return prevRank;
	}
	public void setPrevRank(List<String> prevRank) {
		this.prevRank = prevRank;
	}
	
	public List<String> getPickupAreaCodes() {
		return pickupAreaCodes;
	}
	public void setPickupAreaCodes(List<String> pickupAreaCodes) {
		this.pickupAreaCodes = pickupAreaCodes;
	}
	
	public List<Long> getCurrentCounts() {
		return currentCounts;
	}
	public void setCurrentCounts(List<Long> currentCounts) {
		this.currentCounts = currentCounts;
	}
	
	
}
