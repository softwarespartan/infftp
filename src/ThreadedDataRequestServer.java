
import ipworks.IPWorksException;

import java.io.*;
import java.net.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;


class ThreadedDataRequestServer implements Runnable {
	
	private final int MAX_NUM_WAITING_CONNECTIONS = 100;
	
	private final ServerSocket serverSocket;
	
	private final ExecutorService pool;
	
	private final RoutingServer routingServer;

	public ThreadedDataRequestServer(int port, 
									 RoutingServer routingServer) 
	throws IOException {
		serverSocket = new ServerSocket(port, MAX_NUM_WAITING_CONNECTIONS);
		pool = Executors.newCachedThreadPool();
		this.routingServer = routingServer;
	}

	public void run() { 
		try {
			while(true) {
				pool.execute(new DataRequestHandler(
						                 serverSocket.accept(),
							 			 this.routingServer
							 			 ));
			}
		} catch (IOException ex) {
			pool.shutdown();
		}
	}
	
	void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
				pool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}
}

class DataRequestHandler implements Runnable {
	
	private final Socket connection;
	
	private final DataRequestStringParser parser;
	
	DataRequestHandler(Socket socket,RoutingServer routingServer) { 
		this.connection = socket; 
		this.parser = new DataRequestStringParser(routingServer);
	}

	public void run() {

		try {
			BufferedReader inputStream = null;

			// get the connection output stream
			Writer out = new OutputStreamWriter(connection.getOutputStream());

			// fully initialize the network input from new socket
			inputStream = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));

			// create new string buffer for the xml data
			StringBuffer xmlData = new StringBuffer();

			// integer holds a byte's worth of data
			int c;

			// read the input stream until nothing left (i.e returns -1)
			// read the data one byte at a time ...
			while ((c = inputStream.read()) != -1 && inputStream.ready()) {
				// append any characters (bytes) to the string buffer
				xmlData.append((char) c);
			}

			// make a string outta the string buffer
			String xmlDataString = xmlData.toString().trim();

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

		} catch (IOException ex) {

			// nothing to do about it really

		} finally {

			if (connection != null){
				try {
					connection.close();
				} catch (IOException e) {
					// e.printStackTrace();
				}
			}
		} // end try {

	} // end run()
}


