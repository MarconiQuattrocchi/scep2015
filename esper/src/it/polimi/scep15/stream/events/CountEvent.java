package it.polimi.scep15.stream.events;


public class CountEvent {
	
	private long count;
	private long pickupDate;
	private long dropoffDate;
	private long ts;
	private String routeCode;
	
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
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
	public String getRouteCode() {
		return routeCode;
	}
	public void setRouteCode(String routeCode) {
		this.routeCode = routeCode;
	}
	public String toString(){
		return "event-cont: "+routeCode+"-"+count;
	}
}
