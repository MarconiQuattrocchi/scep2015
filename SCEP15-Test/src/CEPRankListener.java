import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;


public class CEPRankListener implements UpdateListener {

	EPRuntime cepRT;
	public void update(EventBean[] newData, EventBean[] oldData) {
		for(EventBean e : newData)
			System.out.println(e.getUnderlying());
	}
}

