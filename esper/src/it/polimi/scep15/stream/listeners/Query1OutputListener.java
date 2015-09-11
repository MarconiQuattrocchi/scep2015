package it.polimi.scep15.stream.listeners;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;


public class Query1OutputListener implements UpdateListener {

	private static Logger logger = Logger.getLogger("log");

	@SuppressWarnings("unchecked")
	public void update(EventBean[] newData, EventBean[] oldData) {

		for(EventBean e : newData){
			Map<String, Object> r = (Map<String, Object>) e.getUnderlying();
			String res=r.get("pickupDate")+", "+r.get("dropoffDate")+", ";
			List<String> rank = (List<String>)r.get("currentRank");
			if(rank.size()<10)
				break;
			List<Long> counts = (List<Long>)r.get("currentCounts");
			for(int i =0; i<rank.size(); i++){
				res+=rank.get(i)+"("+counts.get(i)+"), ";
			}
			res+=System.currentTimeMillis()-(Long)r.get("ts");
			logger.info("[Query1] "+res);
		}
	}
	
}

