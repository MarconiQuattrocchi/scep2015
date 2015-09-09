package it.polimi.scep15.stream.main;
import it.polimi.scep15.net.SocketDataReceiver;
import it.polimi.scep15.stream.events.CountEvent;
import it.polimi.scep15.stream.events.EntryEvent;
import it.polimi.scep15.stream.events.EntryEvent2;
import it.polimi.scep15.stream.events.RankEvent;
import it.polimi.scep15.stream.listeners.CEPCountListener;
import it.polimi.scep15.stream.listeners.Query1OutputListener;
import it.polimi.scep15.stream.listeners.Query2OutputListener;
import it.polimi.scep15.utils.SourceDataListener;

import java.util.ArrayList;

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
		String query11 = null;
		String query12 = null;
		String query13 = null;

		String query21 = null;
		String query22 = null;
		String query24 = null;
		
		cepConfig = new Configuration();
		cepConfig.addEventType("EntryEvent", EntryEvent.class.getName());
		cepConfig.addEventType("EntryEvent2", EntryEvent2.class.getName());
		cepConfig.addEventType("RankEvent", RankEvent.class.getName());
		cepConfig.addEventType("CountEvent", CountEvent.class.getName());

		query11 = "insert into CountEvent select count(*) as count, routeCode, max(ts) as ts, max(pickupDate) as pickupDate, max(dropoffDate) as dropoffDate "
				+ "from EntryEvent.win:time(5 sec) "
				+ "where pickupAreaX>0 and pickupAreaX<=500 and pickupAreaY>0 and pickupAreaY<=500 and "
				+ "dropoffAreaX>0 and dropoffAreaX<=500 and dropoffAreaY>0 and dropoffAreaY<=500 "
				+ "group by routeCode having count(*)>0 output all every 4 seconds order by count(*) desc, routeCode desc limit 10";
		
		query12 = "insert into RanksEvent select prev(rank) as prevRank, rank as currentRank, "
				+ "counts as currentCounts, pickupDate as pickupDate, dropoffDate as dropoffDate, ts as ts from RankEvent.win:length(2)";
		
		query13 = "select * from RanksEvent.win:length(1) where currentRank != prevRank output all every 1 events";

		query21 = "insert into Profit select median(fareAmount+tipAmount) as profits, pickupAreaCode as area, max(ts) as ts, max(dropoffDate) as dropoffDate, max(pickupDate) as pickupDate "
				+ "from EntryEvent2.win:time(5 sec) "
				+ "where pickupAreaX>0 and pickupAreaX<=600 and pickupAreaY>0 and pickupAreaY<=600 and "
				+ "dropoffAreaX>0 and dropoffAreaX<=600 and dropoffAreaY>0 and dropoffAreaY<=600 "
				+ "group by pickupAreaCode having max(ts) > 0"
				+ "output all every 4 seconds order by profits desc" ;

		query22 = "insert into CountEmptyTaxi select count(*) as empties, dropoffAreaCode as area, max(ts) as ts, max(dropoffDate) as dropoffDate, max(pickupDate) as pickupDate "
				+ "from EntryEvent2.std:groupwin(medallion).win:time(5 sec) "
				+ "where pickupAreaX>0 and pickupAreaX<=600 and pickupAreaY>0 and pickupAreaY<=600 and "
				+ "dropoffAreaX>0 and dropoffAreaX<=600 and dropoffAreaY>0 and dropoffAreaY<=600 "
				+ "group by dropoffAreaCode having max(ts) > 0 output all every 4 seconds order by empties desc";
		
		//query23 = "insert into CountEmptyTaxi select distinct count(*) as empties, dropoffAreaCode as area, max(ts) as ts, max(dropoffDate) as dropoffDate, max(pickupDate) as pickupDate "
		//		+ "from EmptyTaxi.win:time(5 sec) "
		//		+ "group by dropoffAreaCode having max(ts) > 0 output all every 5 seconds order by empties desc";

		query24 = "insert into ProfitabilityRank select c.area as area, max(c.ts) as ts, max(p.profits/c.empties) as profitability, "
				+ "max(p.profits) as profit, max(c.empties) as empties, max(max(c.dropoffDate, p.dropoffDate)) as dropoffDate, max(max(c.pickupDate, p.pickupDate)) as pickupDate "
				+ "from CountEmptyTaxi.win:time(5 sec) as c, Profit.win:time(5 sec) as p "
				+ "where c.area = p.area "
				+ "group by c.area having max(c.ts)>0" 
				+ "output all every 4 seconds order by profitability desc, area desc limit 10";
		
		
		
		EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEP", cepConfig);
	
		EPRuntime cepRT = cep.getEPRuntime();
		EPAdministrator cepAdm = cep.getEPAdministrator();
		
		EPStatement cepStatement =	cepAdm.createEPL(query11);
		
		CEPCountListener cepL11 = new CEPCountListener();
		cepL11.setCepRT(cepRT);
		cepStatement.addListener(cepL11);
		
		cepAdm.createEPL(query12);
		
		EPStatement cepStatement13 = cepAdm.createEPL(query13);
		Query1OutputListener cepL13 = new Query1OutputListener();
		cepStatement13.addListener(cepL13);	
	
		
		cepAdm.createEPL(query21);
		//EPStatement cepStatement21 =cepAdm.createEPL(query21);
		//CEPPrintlnListener cepL21 = new CEPPrintlnListener();
		//cepStatement21.addListener(cepL21);	
		
		
		cepAdm.createEPL(query22);
		//EPStatement cepStatement22 =cepAdm.createEPL(query22);
		//CEPPrintlnListener cepL22 = new CEPPrintlnListener();
		//cepStatement22.addListener(cepL22);	
		
		//cepAdm.createEPL(query23);
		
	
		EPStatement cepStatement24 =cepAdm.createEPL(query24);
		Query2OutputListener cepL24 = new Query2OutputListener();
		cepStatement24.addListener(cepL24);	
		
		generateEmptyRank(cepRT);

		SocketDataReceiver socketServer = new SocketDataReceiver();
		SourceDataListener dataListener=new SourceDataListener();
		dataListener.setCepRT(cepRT);
		socketServer.setListener(dataListener);
		new Thread(socketServer).start();		
	}

	
}



