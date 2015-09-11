package it.polimi.scep15.utils;

import it.polimi.scep15.net.RemoteUpdateListener;
import it.polimi.scep15.stream.events.EntryEvent;
import it.polimi.scep15.stream.events.EntryEvent2;
import it.polimi.scep15.stream.events.RankEvent;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.espertech.esper.client.EPRuntime;


public class SourceDataListener implements RemoteUpdateListener {
	
	private Executor ex = Executors.newFixedThreadPool(1);
	private EPRuntime cepRT;
	HashMap<String, String> map = new HashMap<String, String>();
	@Override
	public synchronized void update(String data) {
		//System.out.println(i+") Received entry: "+data);
		try {
			
			//int i = 0;
			
			final EntryEvent e = EntryParser.parseEntry(data);
			final EntryEvent2 e2 = EntryParser.parseEntry2(data);
			
			//System.out.println(i+") Received entry: "+data);
			map.put(e2.getMedallion(), e2.getDropoffAreaCode());
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

	@Override
	public void newConnection() {
		map.clear();
		generateEmptyRank(cepRT);		
	}
	
	private static void generateEmptyRank(EPRuntime cepRT) {
		RankEvent r = new RankEvent();
		r.setCounts(new ArrayList<Long>());
		r.setRank(new ArrayList<String>());
		cepRT.sendEvent(r);
	}

	@Override
	public void endConnection() {
		int i = 0;
		for(String s : map.values()){
			if(s.equals("(324.315)"))
				i++;
		}
		
		System.out.println("AAAAA: "+i);
	}

}
