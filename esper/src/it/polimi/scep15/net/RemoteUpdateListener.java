package it.polimi.scep15.net;

public interface RemoteUpdateListener {
	public void update(String data);
	public void newConnection();
	public void endConnection();
}
