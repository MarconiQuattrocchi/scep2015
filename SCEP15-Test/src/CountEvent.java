
public class CountEvent {
	long count;
	String word;
	
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	
	public String toString(){
		return "event-cont: "+word+"-"+count;
	}
}
