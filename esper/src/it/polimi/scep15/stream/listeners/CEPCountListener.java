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
		long pickupDate=0;
		long dropoffDate=0;
		long ts=0;
		for (EventBean e : newData) {
				CountEvent c = (CountEvent) e.getUnderlying();
				rank.add(c.getRouteCode());
				counts.add(c.getCount());
				if(ts < c.getTs())
				{
					pickupDate=c.getPickupDate();
					dropoffDate=c.getDropoffDate();
					ts = c.getTs();
				}
		}
		
		if(!rank.isEmpty()){
			RankEvent ev = new RankEvent();
			ev.setRank(rank);
			ev.setCounts(counts);
			ev.setDropoffDate(dropoffDate);
			ev.setPickupDate(pickupDate);
			ev.setTs(ts);
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