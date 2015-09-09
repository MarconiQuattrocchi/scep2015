package it.polimi.scep15.stream.listeners;
import java.util.List;
import java.util.Map;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;


public class Query1OutputListener implements UpdateListener {

	@SuppressWarnings("unchecked")
	public void update(EventBean[] newData, EventBean[] oldData) {

		for(EventBean e : newData){
			Map<String, Object> r = (Map<String, Object>) e.getUnderlying();
			String res=r.get("pickupDate")+", "+r.get("dropoffDate")+", ";
			List<String> rank = (List<String>)r.get("currentRank");
			List<Long> counts = (List<Long>)r.get("currentCounts");
			for(int i =0; i<rank.size(); i++){
				res+=rank.get(i)+"("+counts.get(i)+"), ";
			}
			res+=System.currentTimeMillis()-(Long)r.get("ts");
			System.out.println("[Query1] "+res);
		}
	}
	
}

