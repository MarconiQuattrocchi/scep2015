package it.polimi.scep15.utils;

import it.polimi.scep15.stream.events.EntryEvent;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;


public class EntryParser {
	
	private static Map<String, Double> TOP_LEFT_CELL_CENTER;
	static{
		TOP_LEFT_CELL_CENTER= new HashMap<String, Double>();
		TOP_LEFT_CELL_CENTER.put("latitude", 41.474937);
		TOP_LEFT_CELL_CENTER.put("longitude", -74.913585);
	}
	private static double LATITUDE_STEP = 0.004491556;
	private static double LONGITUDE_STEP = 0.005986;
	
	public static EntryEvent parse(String data) throws ParseException{
		
		String[] groups = data.split(",");
		if(groups.length!=17)
			return null;
		
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		EntryEvent e = new EntryEvent();
		
		e.setMedallion(groups[0]);
		e.setHackLicense(groups[1]);
		e.setPickupDate(format.parse(groups[2]));
		e.setDropoffDate(format.parse(groups[3]));
		e.setTripTime(Integer.parseInt(groups[4]));
		e.setTripDistance(Float.parseFloat(groups[5]));
		e.setPickupAreaY(getAreaY(Float.parseFloat(groups[6])));
		e.setPickupAreaX(getAreaX(Float.parseFloat(groups[7])));
		e.setPickupAreaCode("("+e.getPickupAreaX()+"."+e.getPickupAreaY()+")");
		e.setDropoffAreaY(getAreaY(Float.parseFloat(groups[8])));
		e.setDropoffAreaX(getAreaX(Float.parseFloat(groups[9])));
		e.setDropoffAreaCode("("+e.getDropoffAreaX()+"."+e.getDropoffAreaY()+")");
		e.setRouteCode("["+e.getPickupAreaCode()+","+e.getDropoffAreaCode()+"]");
		e.setPaymentType(groups[10]);
		e.setFareAmout(Float.parseFloat(groups[11]));
		e.setSurcharge(Float.parseFloat(groups[12]));
		e.setMtaTax(Float.parseFloat(groups[13]));
		e.setTipAmount(Float.parseFloat(groups[14]));
		e.setTollsAmount(Float.parseFloat(groups[15]));
		e.setTotalAmount(Float.parseFloat(groups[16]));
		e.setTs(System.currentTimeMillis());
		
		
		
		return e;
	}

	
	private static int getAreaY(float longitude){
	    double f=(longitude - TOP_LEFT_CELL_CENTER.get("longitude") - LONGITUDE_STEP/2 )/LONGITUDE_STEP;
	    if(f>=0)
			return (int)f+1;
		return -1;
	}
	
	private static int getAreaX(float latitude){
		double f = (TOP_LEFT_CELL_CENTER.get("latitude") + LATITUDE_STEP/2 - latitude)/LATITUDE_STEP;
		if(f>=0)
			return (int)f+1;
		return -1;
	}
}
