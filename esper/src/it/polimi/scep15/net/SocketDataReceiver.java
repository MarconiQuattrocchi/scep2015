package it.polimi.scep15.net;
import it.polimi.scep15.utils.SourceDataListener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;


public class SocketDataReceiver implements Runnable {
	
	private RemoteUpdateListener listener;
    ServerSocket serverSocket;
   
	public RemoteUpdateListener getListener() {
		return listener;
	}

	public void setListener(RemoteUpdateListener listener) {
		this.listener = listener;
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(4343);
			loop();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void loop(){
		try ( 
		    Socket clientSocket = serverSocket.accept();
		    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		) {
		    String inputLine;
		    listener.newConnection();
		    while ((inputLine = in.readLine()) != null) {
		    	if(!inputLine.equals(""))
		    		listener.update(inputLine);
		    }
		    listener.endConnection();
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
