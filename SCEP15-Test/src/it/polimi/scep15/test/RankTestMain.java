package it.polimi.scep15.test;
import it.polimi.scep15.test.events.CountEvent;
import it.polimi.scep15.test.events.RankEvent;
import it.polimi.scep15.test.events.RanksEvent;
import it.polimi.scep15.test.events.WordEvent;
import it.polimi.scep15.test.listeners.CEPCountListener;
import it.polimi.scep15.test.listeners.CEPRankListener;

import java.util.ArrayList;
import java.util.Random;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class RankTestMain {

	private static Random generator = new Random();
	
	public static void generateRandomWords(EPRuntime cepRT) throws InterruptedException{
		String[] ss = {"a", "b"};
		int i = generator.nextInt(ss.length);
		WordEvent e = new WordEvent();
		e.setWord(ss[i]);
		System.out.println("Sending Event:" + e);
		cepRT.sendEvent(e);
		Thread.sleep(1000);
	}

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
		cepConfig.addEventType("WordEvent", WordEvent.class.getName());
		cepConfig.addEventType("RankEvent", RankEvent.class.getName());
		cepConfig.addEventType("RanksEvent", RanksEvent.class.getName());

		cepConfig.addEventType("CountEvent", CountEvent.class.getName());

		query = "insert into CountEvent select count(*) as count, word as word from WordEvent.win:time(2 sec) "
				+ "group by word output all every 2 seconds order by count(*) desc, word desc";
		
		query2 = "insert into RanksEvent select prev(rank) as prevRank, rank as currentRank, prev(counts) as prevCounts, "
				+ "counts as currentCounts from RankEvent.win:length(2)";
		
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
		
		for (int i = 0; i < 100; i++) {
			generateRandomWords(cepRT);
		}
		
		Thread.sleep(100000);
		
	}

	
}



