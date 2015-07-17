package it.polimi.scep15.stream.events;
import java.util.Date;


public class EntryEvent {
	
	private String medallion;
	private String hackLicense;
	private Date pickupDate;
	private Date dropoffDate;
	private int tripTime;
	private float tripDistance;
	private int pickupAreaX;
	private int pickupAreaY;
	private String pickupAreaCode;
	private int dropoffAreaX;
	private int dropoffAreaY;
	private String dropoffAreaCode;
	private String paymentType;
	private float fareAmout;
	private float surcharge;
	private float mtaTax;
	private float tipAmount;
	private float tollsAmount;
	private float totalAmount;
	
	
	public String getMedallion() {
		return medallion;
	}
	public void setMedallion(String medallion) {
		this.medallion = medallion;
	}
	public String getHackLicense() {
		return hackLicense;
	}
	public void setHackLicense(String hackLicense) {
		this.hackLicense = hackLicense;
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
	public int getTripTime() {
		return tripTime;
	}
	public void setTripTime(int tripTime) {
		this.tripTime = tripTime;
	}
	public float getTripDistance() {
		return tripDistance;
	}
	public void setTripDistance(float tripDistance) {
		this.tripDistance = tripDistance;
	}

	public int getPickupAreaX() {
		return pickupAreaX;
	}
	public void setPickupAreaX(int pickupAreaX) {
		this.pickupAreaX = pickupAreaX;
	}
	public int getPickupAreaY() {
		return pickupAreaY;
	}
	public void setPickupAreaY(int pickupAreaY) {
		this.pickupAreaY = pickupAreaY;
	}
	public String getPickupAreaCode() {
		return pickupAreaCode;
	}
	public void setPickupAreaCode(String pickupAreaCode) {
		this.pickupAreaCode = pickupAreaCode;
	}
	public int getDropoffAreaX() {
		return dropoffAreaX;
	}
	public void setDropoffAreaX(int dropoffAreaX) {
		this.dropoffAreaX = dropoffAreaX;
	}
	public int getDropoffAreaY() {
		return dropoffAreaY;
	}
	public void setDropoffAreaY(int dropoffAreaY) {
		this.dropoffAreaY = dropoffAreaY;
	}
	public String getDropoffAreaCode() {
		return dropoffAreaCode;
	}
	public void setDropoffAreaCode(String dropoffAreaCode) {
		this.dropoffAreaCode = dropoffAreaCode;
	}
	public String getPaymentType() {
		return paymentType;
	}
	public void setPaymentType(String paymentType) {
		this.paymentType = paymentType;
	}
	public float getFareAmout() {
		return fareAmout;
	}
	public void setFareAmout(float fareAmout) {
		this.fareAmout = fareAmout;
	}
	public float getSurcharge() {
		return surcharge;
	}
	public void setSurcharge(float surcharge) {
		this.surcharge = surcharge;
	}
	public float getMtaTax() {
		return mtaTax;
	}
	public void setMtaTax(float mtaTax) {
		this.mtaTax = mtaTax;
	}
	public float getTipAmount() {
		return tipAmount;
	}
	public void setTipAmount(float tipAmount) {
		this.tipAmount = tipAmount;
	}
	public float getTollsAmount() {
		return tollsAmount;
	}
	public void setTollsAmount(float tollsAmount) {
		this.tollsAmount = tollsAmount;
	}
	public float getTotalAmount() {
		return totalAmount;
	}
	public void setTotalAmount(float totalAmount) {
		this.totalAmount = totalAmount;
	}
}
