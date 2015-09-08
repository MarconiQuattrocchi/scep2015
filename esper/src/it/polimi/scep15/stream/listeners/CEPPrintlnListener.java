package it.polimi.scep15.stream.listeners;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class CEPPrintlnListener implements UpdateListener {

	public void update(EventBean[] newData, EventBean[] oldData) {
		for(EventBean e : newData){
			System.out.println(e.getUnderlying());
		}
		
		System.out.println("");

		for(EventBean e : oldData){
			System.out.println(e.getUnderlying());
		}
		
		System.out.println("");

	}
	
}
