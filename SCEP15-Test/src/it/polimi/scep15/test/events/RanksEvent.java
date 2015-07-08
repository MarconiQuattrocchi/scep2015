package it.polimi.scep15.test.events;
import java.util.List;


public class RanksEvent {
	List<String> prevRank;
	List<String> currentRank;
	List<Long> prevCounts;
	List<Long> currentCounts;

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
	public List<Long> getPrevCounts() {
		return prevCounts;
	}
	public void setPrevCounts(List<Long> prevCounts) {
		this.prevCounts = prevCounts;
	}
	public List<Long> getCurrentCounts() {
		return currentCounts;
	}
	public void setCurrentCounts(List<Long> currentCounts) {
		this.currentCounts = currentCounts;
	}
	public String toString(){
		return "New Rank: "+currentRank+"   (currentCounts: "+currentCounts+", previousRank: "+prevRank+")"; //, previousCounts: "+prevCounts+")";
	}
}
