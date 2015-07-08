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
			ev.rank=rank;
			ev.counts=counts;
			System.out.println("Sending rank: "+ev);
			cepRT.sendEvent(ev);
		}
	}
}