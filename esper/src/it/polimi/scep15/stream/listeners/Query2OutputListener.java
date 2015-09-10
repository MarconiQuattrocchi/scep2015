package it.polimi.scep15.stream.listeners;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class Query2OutputListener implements UpdateListener {

	private static Logger logger = Logger.getLogger("log");

	@SuppressWarnings("unchecked")
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		long now = System.currentTimeMillis();
		long maxts=0;
		long maxdd=0;
		long maxpd=0;
		
		String res="";
		DecimalFormat numberFormat = new DecimalFormat("#.0");
		ArrayList<String> areas = new ArrayList<String>();
		for(int i=0; i<newData.length; i++){
			Map<String, Object> e = (Map<String, Object>) newData[i].getUnderlying();
			if(areas.contains((String)e.get("area"))){
				;//logger.info("ERROR");
			}
			areas.add((String)e.get("area"));
			res+=", <"+e.get("area")+", "+e.get("empties")+", "+numberFormat.format(e.get("profit"))+", "+numberFormat.format(e.get("profitability"))+">";
			long ts=(long) e.get("ts");
			long pd=(long) e.get("pickupDate");
			long dd=(long) e.get("dropoffDate");
			if(ts>maxts)
				maxts=ts;
			if(dd>maxdd)
				maxdd=dd;
			if(pd>maxpd)
				maxpd=pd;
		}
		
		res=maxpd+", "+maxdd+res+", "+(now-maxts);
		logger.info("[Query2] "+res);	
	}

}
