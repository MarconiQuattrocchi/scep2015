package it.polimi.scep15.utils;

import it.polimi.scep15.net.RemoteUpdateListener;
import it.polimi.scep15.stream.events.EntryEvent;

import java.text.ParseException;

import com.espertech.esper.client.EPRuntime;


public class SourceDataListener implements RemoteUpdateListener {
	

	private EPRuntime cepRT;

	@Override
	public synchronized void update(String data) {
		//System.out.println(i+") Received entry: "+data);
		try {
			
			
			EntryEvent e = EntryParser.parse(data);
			
			//System.out.println(i+") Received entry: "+data);

			if(cepRT!=null){
				cepRT.sendEvent(e);
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}

	public EPRuntime getCepRT() {
		return cepRT;
	}

	public void setCepRT(EPRuntime cepRT) {
		this.cepRT = cepRT;
	}

}
