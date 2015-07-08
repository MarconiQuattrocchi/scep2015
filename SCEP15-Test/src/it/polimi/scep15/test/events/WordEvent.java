package it.polimi.scep15.test.events;


public class WordEvent {
	String word;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}
	
	public String toString(){
		return "event-word: "+word;
	}
}	
