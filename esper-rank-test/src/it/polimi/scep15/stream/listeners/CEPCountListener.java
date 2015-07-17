package it.polimi.scep15.stream.listeners;
import it.polimi.scep15.stream.events.CountEvent;
import it.polimi.scep15.stream.events.RankEvent;

import java.util.ArrayList;
import java.util.List;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class CEPCountListener implements UpdateListener {
	
	EPRuntime cepRT;
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		List<String> rank = new ArrayList<String>();
		List<Long> counts = new ArrayList<Long>();
		List<String> pickupAreaCodes = new ArrayList<String>();
		
		for (EventBean e : newData) {
				CountEvent c = (CountEvent) e.getUnderlying();
				rank.add(c.getDropoffAreaCode());
				pickupAreaCodes.add(c.getPickupAreaCode());
				counts.add(c.getCount());
		}

		if(!rank.isEmpty()){
			RankEvent ev = new RankEvent();
			ev.setRank(rank);
			ev.setCounts(counts);
			ev.setPickupAreaCodes(pickupAreaCodes);
			//System.out.println("Sending rank: "+ev);
			cepRT.sendEvent(ev);
		}
	}
	
	public EPRuntime getCepRT() {
		return cepRT;
	}
	public void setCepRT(EPRuntime cepRT) {
		this.cepRT = cepRT;
	}
}