package it.polimi.scep15.stream.main;
import java.util.ArrayList;

import it.polimi.scep15.net.SocketDataReceiver;
import it.polimi.scep15.stream.events.CountEvent;
import it.polimi.scep15.stream.events.EntryEvent;
import it.polimi.scep15.stream.events.RankEvent;
import it.polimi.scep15.stream.events.RanksEvent;
import it.polimi.scep15.stream.listeners.CEPCountListener;
import it.polimi.scep15.stream.listeners.CEPRankListener;
import it.polimi.scep15.utils.SourceDataListener;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class StreamMain {
	
	private static void generateEmptyRank(EPRuntime cepRT) {
		RankEvent r = new RankEvent();
		r.setCounts(new ArrayList<Long>());
		r.setRank(new ArrayList<String>());
		cepRT.sendEvent(r);
	}
	
	
	public static void main(String[] args) throws InterruptedException {
		
		Configuration cepConfig = null;
		String query = null;
		String query2 = null;
		String query3 = null;

		cepConfig = new Configuration();
		cepConfig.addEventType("EntryEvent", EntryEvent.class.getName());
		cepConfig.addEventType("RankEvent", RankEvent.class.getName());
		cepConfig.addEventType("RanksEvent", RanksEvent.class.getName());

		cepConfig.addEventType("CountEvent", CountEvent.class.getName());

		query = "insert into CountEvent select count(*) as count, routeCode as routeCode, "
				+ "pickupDate as pickupDate, dropoffDate as dropoffDate, ts as ts from EntryEvent.win:time(5 sec) "
				+ "where pickupAreaX>0 and pickupAreaX<=500 and pickupAreaY>0 and pickupAreaY<=500 and "
				+ "dropoffAreaX>0 and dropoffAreaX<=500 and dropoffAreaY>0 and dropoffAreaY<=500 "
				+ "group by routeCode output all every 1 events order by count(*) desc, routeCode desc limit 10";
		
		query2 = "insert into RanksEvent select prev(rank) as prevRank, rank as currentRank, "
				+ "counts as currentCounts, pickupDate as pickupDate, dropoffDate as dropoffDate, ts as ts from RankEvent.win:length(2)";
		
		query3 = "select * from RanksEvent.win:length(1) where currentRank != prevRank output all every 1 events";

		EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEP", cepConfig);
		
		EPRuntime cepRT = cep.getEPRuntime();
		EPAdministrator cepAdm = cep.getEPAdministrator();
		EPStatement cepStatement =	cepAdm.createEPL(query);
		
		CEPCountListener cepL = new CEPCountListener();
		cepL.setCepRT(cepRT);
		cepStatement.addListener(cepL);
		
		cepAdm.createEPL(query2);
		
		EPStatement cepStatement3 =	cepAdm.createEPL(query3);
		CEPRankListener cepL3 = new CEPRankListener();
		cepL3.setCepRT(cepRT);
		cepStatement3.addListener(cepL3);	

		generateEmptyRank(cepRT);

		SocketDataReceiver socketServer = new SocketDataReceiver();
		SourceDataListener dataListener=new SourceDataListener();
		dataListener.setCepRT(cepRT);
		socketServer.setListener(dataListener);
		new Thread(socketServer).start();		
	}

	
}



