package it.polimi.scep15.utils;

import it.polimi.scep15.net.RemoteUpdateListener;
import it.polimi.scep15.stream.events.EntryEvent;
import it.polimi.scep15.stream.events.EntryEvent2;

import java.text.ParseException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.espertech.esper.client.EPRuntime;


public class SourceDataListener implements RemoteUpdateListener {
	
	private Executor ex = Executors.newFixedThreadPool(1);
	private EPRuntime cepRT;

	@Override
	public synchronized void update(String data) {
		//System.out.println(i+") Received entry: "+data);
		try {
			
			//int i = 0;
			
			final EntryEvent e = EntryParser.parseEntry(data);
			final EntryEvent2 e2 = EntryParser.parseEntry2(data);
			
			//System.out.println(i+") Received entry: "+data);
		
			if(cepRT!=null){
				ex.execute(new Runnable(){
					
					@Override
					public void run() {
						cepRT.sendEvent(e);
					}
				});
				ex.execute(new Runnable(){

					@Override
					public void run() {
						cepRT.sendEvent(e2);
						
					}
				});
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
