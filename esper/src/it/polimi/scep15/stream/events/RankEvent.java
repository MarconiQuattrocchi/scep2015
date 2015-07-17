package it.polimi.scep15.stream.events;
import java.util.Date;
import java.util.List;


public class RankEvent {
	
	private List<String> rank;
	private List<Long> counts;
	
	private Date pickupDate;
	private Date dropoffDate;
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

	public String toString(){
		return "{"+rank+","+counts+"}";
	}
}
