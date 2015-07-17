package it.polimi.scep15.utils;

import java.text.ParseException;

import com.espertech.esper.client.EPRuntime;

import it.polimi.scep15.net.RemoteUpdateListener;
import it.polimi.scep15.stream.events.EntryEvent;



public class SourceDataListener implements RemoteUpdateListener {
	
	private EPRuntime cepRT;
	
	@Override
	public void update(String data) {
		System.out.println("Received entry: "+data);
		try {
			EntryEvent e = EntryParser.parse(data);
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
