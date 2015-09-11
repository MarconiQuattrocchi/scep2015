package it.polimi.scep15.net;
import it.polimi.scep15.utils.SourceDataListener;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;


public class SocketDataReceiver implements Runnable {
	
	private RemoteUpdateListener listener;
	
	public RemoteUpdateListener getListener() {
		return listener;
	}

	public void setListener(RemoteUpdateListener listener) {
		this.listener = listener;
	}

	@Override
	public void run() {
		loop();
	}
	
	public void loop(){
		int port = 4343;
		try ( 
		    ServerSocket serverSocket = new ServerSocket(port);
		    Socket clientSocket = serverSocket.accept();
		    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		) {
		    String inputLine;
		    
		    while ((inputLine = in.readLine()) != null) {
		    	if(!inputLine.equals(""))
		    		listener.update(inputLine);
		    }
		    loop();
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		SocketDataReceiver r = new SocketDataReceiver();
		r.setListener(new SourceDataListener());
		new Thread(r).start();
	}
	
	
}
