package it.polimi.scep15.stream.events;

import java.util.Date;

public class CountEvent {
	
	private long count;
	private Date pickupDate;
	private Date dropoffDate;
	private String pickupAreaCode;
	private String dropoffAreaCode;
	
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
	public String getPickupAreaCode() {
		return pickupAreaCode;
	}
	public void setPickupAreaCode(String pickupAreaCode) {
		this.pickupAreaCode = pickupAreaCode;
	}
	public String getDropoffAreaCode() {
		return dropoffAreaCode;
	}
	public void setDropoffAreaCode(String dropoffAreaCode) {
		this.dropoffAreaCode = dropoffAreaCode;
	}
	public String toString(){
		return "event-cont: "+dropoffAreaCode+"-"+count;
	}
}
