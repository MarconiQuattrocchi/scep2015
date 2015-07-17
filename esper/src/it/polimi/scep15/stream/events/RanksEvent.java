package it.polimi.scep15.stream.events;
import java.util.Date;
import java.util.List;


public class RanksEvent {
	
	private List<String> prevRank;
	private List<String> currentRank;
	private List<Long> currentCounts;
	
	private Date pickupDate;
	private Date dropoffDate;
	private long ts;
	
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
	
	public List<Long> getCurrentCounts() {
		return currentCounts;
	}
	
	public void setCurrentCounts(List<Long> currentCounts) {
		this.currentCounts = currentCounts;
	}
	
	
	public Date getPickupDate() {
		return pickupDate;
	}
	public void setPickupDate(Date pickupDate) {
		this.pickupDate = pickupDate;
	}
	public Date getDropoffDate() {
		return dropoffDate;
	}
	public void setDropoffDate(Date dropoffDate) {
		this.dropoffDate = dropoffDate;
	}
	
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	
	
}
