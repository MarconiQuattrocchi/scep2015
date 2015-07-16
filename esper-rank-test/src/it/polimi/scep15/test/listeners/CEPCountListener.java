package it.polimi.scep15.test.listeners;
import it.polimi.scep15.test.events.CountEvent;
import it.polimi.scep15.test.events.RankEvent;

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
		
		for (EventBean e : newData) {
				rank.add(((CountEvent)e.getUnderlying()).getWord());
				counts.add(((CountEvent)e.getUnderlying()).getCount());
		}

		if(!rank.isEmpty()){
			RankEvent ev = new RankEvent();
			ev.setRank(rank);
			ev.setCounts(counts);
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