package it.polimi.scep15.stream.events;

import java.util.Date;

public class CountEvent {
	
	private long count;
	private Date pickupDate;
	private Date dropoffDate;
	private long ts;
	private String routeCode;
	
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
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
