import ipworks.IPWorksException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class g2ServerDataManager {
	
	public final int DATA_REQUEST_PORT = 40002;
	public final int STATS_PORT        = 40003;
	
	//private DataRequestParser  dataRequestParser ;
	private Thread  		   dataRequestServer;
	private ServerFactory      serverParser      ;
	
	private HashMap<String,Server> serverSet;
	private final RoutingServer routingServer = new RoutingServer();
	private Thread  statsServer;
	
	public g2ServerDataManager(String serverInfoFile, 
			 				   String dataProductsFile, 
			 				   String dataRequestsFile){
		
		//this.dataRequestParser = new DataRequestParser(dataRequestsFile,this.routingServer);
		this.serverParser      = new ServerFactory(serverInfoFile,dataProductsFile);
		
		// start the stats server
		try {
			this.statsServer  
				= new Thread(
						new StatsServer(STATS_PORT,this.routingServer));
			this.statsServer.start(); 
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("could not start stats server!");
			//return;
		}
		
		// start the dataRequest server
		try {
			this.dataRequestServer  
				= new Thread(
						new ThreadedDataRequestServer(DATA_REQUEST_PORT,this.routingServer));
			this.dataRequestServer.start();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("could not start dataRequest server!");
			//return;
		}
		
	}
	
	private void parse(){
		
		// parse the xml files
		try {
			this.serverParser.parse();
			//this.dataRequestParser.parse();
		} catch (IPWorksException e) {
			e.printStackTrace();
			return;
		}
		
		// get the objects from the files
		this.serverSet    = this.serverParser.getServers();
		
		// start the routing server
		//this.routingServer.start();
		this.routingServer.initializeWorkerThreads();
		
		// add routing server to each server
		String serverName;
		Iterator<String> iter = this.serverSet.keySet().iterator();
		while(iter.hasNext()){
			
			serverName = iter.next();
			
			// add routing server to server
			this.serverSet.get(serverName).setRoutingServer(this.routingServer);
			
			// add server to routing server
			this.routingServer.addServer(this.serverSet.get(serverName));
		}
	}
	
	public void connect(){
		Iterator<String> iter = this.serverSet.keySet().iterator();
		while(iter.hasNext()){
			this.serverSet.get(iter.next()).connect();
		}
	}
	
	public void shutdown(){
		Iterator<String> iter = this.serverSet.keySet().iterator();
		while(iter.hasNext()){
			this.serverSet.get(iter.next()).disconnect();
		}
		
		// stop routing service
		this.routingServer.interrupt();
	}

	public static void main(String[] args) throws IOException {
		
		g2ServerDataManager g2sdm =  new g2ServerDataManager("./config/servers.xml",
															 "./config/dataProducts.xml",
															 "./config/dataRequest.xml");		
		g2sdm.parse();
		g2sdm.connect();
		//g2sdm.dataRequestParser.parseDataRequests();
				
		// send disconnect to all servers
		//g2sdm.shutdown();
	}

}
