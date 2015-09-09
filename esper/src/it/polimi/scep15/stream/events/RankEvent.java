package it.polimi.scep15.stream.events;
import java.util.List;


public class RankEvent {
	
	private List<String> rank;
	private List<Long> counts;
	
	private long pickupDate;
	private long dropoffDate;
	private long ts;
	
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
	
	public long getPickupDate() {
		return pickupDate;
	}
	public void setPickupDate(long pickupDate) {
		this.pickupDate = pickupDate;
	}
	public long getDropoffDate() {
		return dropoffDate;
	}
	public void setDropoffDate(long dropoffDate) {
		this.dropoffDate = dropoffDate;
	}
	
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}

	public String toString(){
		return "{"+rank+","+counts+"}";
	}
}
