
import ipworks.IPWorksException;

import java.io.*;
import java.net.*;

public class DataRequestServer implements Runnable{

	final int port = 40002;
	
	// where to send the parsed data requests
	private final RoutingServer routingServer;
	
	// the obj that does the heavy lifting
	private final DataRequestStringParser parser;
	
	private boolean shouldQuit = false;
	
	// default constructor
	public DataRequestServer(){
		this.routingServer = null;
		this.parser = new DataRequestStringParser(this.routingServer);
	}
	
	// alt default server
	public DataRequestServer(RoutingServer rs){
		this.routingServer = rs;
		this.parser = new DataRequestStringParser(this.routingServer);
	}

	// hands off from hear ...
	public void run() {
		
		// listen for connections on port
		ServerSocket server = null;

		try {
			
			//initialize the server socket to listen on port 
			server = new ServerSocket(port);

			// the connection
			Socket connection = null;
			
			BufferedReader networkIn = null;

			// do this forever
			while (true){

				try{

					// blocks until gets a poke
					connection = server.accept();

					// get the connection output stream
					Writer out = new OutputStreamWriter(connection.getOutputStream());

					
					// fully initialize the network input from new socket
					networkIn = new BufferedReader(
									new InputStreamReader(
											connection.getInputStream() ) );
						
					// create new string buffer for the xml data
					StringBuffer xmlData = new StringBuffer();
					
					// integer holds a byte's worth of data
					int c;
					
					// read the input stream until nothing left (i.e returns -1)
					// read the data one byte at a time ...
					while((c = networkIn.read()) != -1 && networkIn.ready()){
						// append any characters (bytes) to the string buffer
						xmlData.append((char) c);
					}
					
					// make a string outta the string buffer
					String xmlDataString = xmlData.toString().trim();
					
//					String s;
//					String xmlDataString = "";
//					while((s=networkIn.readLine()) != null){
//						//System.out.println("read the line: "+s);
//						xmlDataString += s;
//						if (s.equals("</RequestDefinition>")) break;
//					}

					String stats = "Error parsing xml data!";
					try {
						this.parser.parse(xmlDataString);
						stats = this.parser.parseDataRequests();
					} catch (IPWorksException e) {
						e.printStackTrace();
					}
					
					out.write(stats);
					out.flush();
					
					connection.close();
					
				} catch (IOException ex){
					
					//nothing to do about it really
				
				} finally {

					if (connection != null)
						try {
							connection.close();
						} catch (IOException e) {
							//e.printStackTrace();
						}
				}	
			}
		}catch (IOException e1) {
			System.err.println("Could not listen for connections on port: "+port);
			e1.printStackTrace();
		} finally {
			if (server != null){
				try {
					server.close();
				} catch (IOException e) {
					System.err.println("Error closing the server listing on port: "+port);
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {

		// start a data request server 
		 (new Thread (new DataRequestServer())).start();
		 System.out.println("fuck ah you whale!");
	}
}
