package it.polimi.scep15.stream.listeners;

import java.util.logging.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class CEPPrintlnListener implements UpdateListener {
	private static Logger logger = Logger.getLogger("log");
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		for(EventBean e : newData){
			logger.info(e.getUnderlying().toString());
		}
		
		logger.info("");

		for(EventBean e : oldData){
			logger.info(e.getUnderlying().toString());
		}
		
		logger.info("");
	}
	
}
