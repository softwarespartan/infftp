

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;

import org.joda.time.DateTime;

public class Server {

	// login stuff
	private String url;
	private String userName;
	private String password;
	private int    port;
	
	// server name or ID
	private String serverName;

	// number of desired simultaneous connections 
	private int    numberOfThreads = 0;

	// number of established connections
	private int    numberOfEstablishedConnections = 0;

	// the set of connection objects/threads
	private ServerConnection[] serverConnections;
	
	// data request are put here 
	private final BlockingQueue<DataRequest> dataRequestQueue;
	
	// data product definitions that this server instance services
	private HashMap<String,String> dataProducts;
	
	//  used to give data requests the appropriate remote path for data product
	private DateKeyPathParser dateKeyPathParser = new DateKeyPathParser();
	
	// if the job can not be serviced here, give back to routingServer
	private RoutingServer routingServer = null;

	// constructors
	public Server(String serverName,
				  String url, 
				  String userName, String password, 
				  int numberOfThreads){
		
		this.serverName = serverName;

		// assign login parameters
		this.url      = url;
		this.userName = userName;
		this.password = password;
		this.port     = 21;

		// check that number of threads is not negative
		if (numberOfThreads > 0){
			this.numberOfThreads = numberOfThreads;
		}else{
			// regular connection
			this.numberOfThreads = 1;
		}
		
		// initialize the data request queue
		this.dataRequestQueue = new LinkedBlockingQueue<DataRequest>();
		
		// initialize the data products map
		this.dataProducts = new HashMap<String,String>();

	}

	public Server(String serverName, String url, String userName, String password, int port, int numberOfThreads){
		
		// set server name
		this.serverName = serverName;

		// assign login parameters
		this.url      = url;
		this.userName = userName;
		this.password = password;
		this.port     = port;		

		// check that number of threads is not negative
		if (numberOfThreads > 0){
			this.numberOfThreads = numberOfThreads;
		}else{
			// regular connection
			this.numberOfThreads = 1;
		}
		
		// initialize the data request queue
		this.dataRequestQueue = new LinkedBlockingQueue<DataRequest>();
		
		// initialize the data products hash map
		this.dataProducts = new HashMap<String,String>();

	}
	
	public RoutingServer getRoutingServer() {
		return routingServer;
	}
	
	public void setRoutingServer(RoutingServer routingServer){
		this.routingServer = routingServer;
	}

	public void setDateKeyPathParser(DateKeyPathParser dateKeyPathParser) {
		this.dateKeyPathParser = dateKeyPathParser;
	}

	public DateKeyPathParser getDateKeyPathParser() {
		return dateKeyPathParser;
	}

	public void addDataProduct(String key,String value){
		this.dataProducts.put(key, value);
	}
	public HashMap<String,String> getDataProducts(){
		return this.dataProducts;
	}
	
	public void setDataProducts(HashMap<String,String> dataProducts){
		this.dataProducts = dataProducts;
	}
	
	// getters for a few properties
	public String getServerName(){
		return this.serverName;
	}
	
	public String getUrl() {
		return url;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}

	public int getPort() {
		return port;
	}

	public int getNumberOfThreads() {
		return numberOfThreads;
	}

	public int getNumberOfEstablishedConnections() {
		return numberOfEstablishedConnections;
	}

	public ServerConnection[] getServerConnections() {
		return serverConnections;
	}

	public BlockingQueue<DataRequest> getDataRequestQueue() {
		return dataRequestQueue;
	}

	public void disconnect() {

		// send interrupt to each serverConnection Thread
		int N = this.numberOfThreads;
		for (int i = 0; i < N; i = i+1){
			if(this.serverConnections[i].isAlive()){
				System.out.println("SERVER ( " +this.url+ " ) removing connection: "+Integer.toString(i+1));
				this.serverConnections[i].interrupt();
			}
		}
	}
	
	public void putToSleep(int N) {
		
		int oneSec = 1000;
		try {
			Thread.sleep(oneSec * N);
		} catch (InterruptedException e) {
			
		}
	}
	
	public void connect(){
		// init connections array to hold numberOfThreads
		this.serverConnections = new ServerConnection[this.numberOfThreads];

		// initialize serverConnection and start them
		for (int i = 0; i < this.numberOfThreads; i = i+1){
			
			try {
			if (this.routingServer == null){
				
				// no routing server ... last stop for jobs
				this.serverConnections[i] = new ServerConnection(this.url,this.port,
																 this.userName,this.password,
																 this.dataRequestQueue);
			}else{
				// add routing server
				this.serverConnections[i] = new ServerConnection(this.url,this.port,
																 this.userName,this.password,
																 this.dataRequestQueue,
																 this.routingServer);
			}
			} catch (IOException e){
				System.err.println(this.serverName+": could not start thread "+i);
			}
			
			this.serverConnections[i].start();
		}
		
		System.out.println(this.getServerName()+" attempted to start "+this.numberOfThreads+" connections");
	}
	
	public Boolean isConnected(){
		if (this.numberOfEstablishedConnections > 0){
			return true;
		}else{
			return false;
		}
	}
	
	public synchronized int getJobCount(){
		return this.dataRequestQueue.size();
	}
	
	public synchronized void submitDataRequest(DataRequest dataRequest){
		
			
		// update service history
		// this lets the routing service know that that 
		// data requests has already attempted to be service here.
		// This is, so that we don't get the request back if it fails 
		// on this server
		dataRequest.addVisitedServer(this.serverName);
		
		// check first for data product definitions
		if (!this.dataProducts.isEmpty()){
						
			// if the server has data product def then check that 
			// the data requests data product is found.
			// lol, that is, make sure this server object can service the 
			// data product associated with this incoming data request
			if (this.dataProducts.containsKey(dataRequest.getDataProduct())){				
				
				
				//System.out.println("FUCK YOU I SERVICE THIS DATA PRODUCT!!!");
				
				// set the remote path template for the data product on this server
				//dataRequest.setRemotePathTemplate(this.dataProducts.get(dataRequest.getDataProduct()));
				
				// OK, now make the remote path template absolute (apply substitutions) 
				String newRemotePath = this.dateKeyPathParser.replaceWithLiterals(this.dataProducts.get(dataRequest.getDataProduct()), 
																				  dataRequest.getEffectiveDate(), 
																				  dataRequest.getKey());
				
				
				// set the final, fully determined, remote path for data product
				dataRequest.setRemotePath(newRemotePath);
				
				// Now need to set the local path since the name of the file could change from server to server
				// for example SOPAC: algo2000.08d.Z where same file at CDDIS is algo2000.08o.Z
				// This information is stored in the data product definition on the server side but needs to be
				// transfered to client side so that the name of the file on the local file system is same name
				// as ftp server. 
				
				// step 1. first isolate the file name from the remote path.
				String remoteFileName = new File(newRemotePath).getName();
				
				// step 2. append this to the local path.  
				// Notice here that we have to be really careful about path separators for different platforms
				// so we'll use the File class to to the path concatenation for us and then as for the str rep.
				// Finally, note that local path template is used since it never has remote file name appended to it
				// if always recycle local path then run the risk of dropping parent dir in path parsing
				
				new File("");
				//       7/13/2010
				// NEW:  if local path ends with "/" or generally speaking a path separator then should append
				//       remote file name to the end of the localPath.  however if the localPath contains an absolute
				//       file name then should translate the localPath with out appending remote file name.
				//  
				// for example:   remoteFilePath: /some/dir/gf113442.CLK.Z
				//                localFilePath:  /some/other/dir/gfz13442.clk.Z
				//
				//                remoteFilePath: /some/dir/gf1gpsdate.CLK.Z
				//                localFilePath:  /some/other/dir/gfzgpsdate.clk.Z
				//
			    // notice here that the gpsdate still translates but that the id gf1 ->> gfz and CLK --> clk
				String newLocalPath;
				if (dataRequest.getLocalPathTemplate().endsWith(File.separator)){

					// append remoteFileName to the end of the localPathTemplet to form abs local path
					newLocalPath = new File(dataRequest.getLocalPathTemplate(),remoteFileName).toString();
					
				} else {
					
					// disregard the remote file name and setit in a predefined way as specified by
					// localPathTemplate.
					newLocalPath = this.dateKeyPathParser.replaceWithLiterals(
							  												  dataRequest.getLocalPathTemplate(), 
							  												  dataRequest.getEffectiveDate(), 
							  												  dataRequest.getKey()
																			 );
				}
				
				// step 3. assign the local path to the data request
				dataRequest.setLocalPath(newLocalPath);
				
				// finally shlep it into the queue
				try{
					this.dataRequestQueue.put(dataRequest);
				} catch (InterruptedException e){
					throw new RuntimeException("add to queue interrupted");
				}
			} else {
			
				System.out.println("SERVER ( "+this.serverName+" ) can not service data product: "+dataRequest.getDataProduct()+ "  Will forward ...");
				this.routingServer.submitDataRequest(dataRequest);
			}
		}
	}
	
	public void print(){
		System.out.println("Server: "+this.serverName);
		System.out.println("\t Address     : "+this.url);
		System.out.println("\t User Name   : "+this.userName);
		System.out.println("\t Password    : "+this.password);
		System.out.println("\t Port        : "+this.port);
		
		String key;
		Iterator<String> dataProductKeys = this.dataProducts.keySet().iterator();
		while(dataProductKeys.hasNext()){			
			key = dataProductKeys.next();
			System.out.println("\t Data Product: "+key+" @ "+this.dataProducts.get(key));
		}

	}
	
	private DateTime getDateWith(int year, int doy){
		
		// temp date object
		DateTime dt = new DateTime();
		
		// set the year
		dt = dt.withYear(year);
		
		// set the day of year
		dt = dt.withDayOfYear(doy);
		
		return dt;
		
	}
	
	public static void main(String[] args){

		int numberOfThreads = 1;
		//Server unavco = new Server("UNAVCO","data-out.unavco.org","anonymous","brown.2179@osu.edu",numberOfThreads); 
		//Server sopac  = new Server("SOPAC","garner.ucsd.edu","anonymous","brown.2179@osu.edu",     numberOfThreads); 
		//Server cddis  = new Server("CDDIS","cddis.gsfc.nasa.gov","anonymous","brown.2179@osu.edu", numberOfThreads); 
		//Server mit    = new Server("MIT","everest.mit.edu","anonymous","brown.2179@osu.edu",5);
		
		// generic dataRequest object
		DataRequest tempDataRequest;
		DataRequest dataRequest = new DataRequest("","","",new DateTime());
		
//		String local;
//		String remote;
//		String ddd;
		
		DateKeyPathParser pathParser = new DateKeyPathParser();
		
//		String local = "/media/fugu/solutions/MIT/2009/doy/mitgpsdate.sp3.Z";
//		String remote = "/pub/MIT_SP3/mitgpsdate.sp3.Z";
		
//		String local = "/media/fugu/solutions/MIT/2009/doy/H09doy_MIT.GLX";
//		String remote = "/pub/MIT_GLL/H09/H09doy_MIT.GLX";
//		
//		String localPath;
//		String remotePath;
//		for (int i = 300; i<=365; i+=1){
//			
//			localPath = pathParser.replaceWithLiterals(local, mit.getDateWith(2009, i), "mit");
//			remotePath = pathParser.replaceWithLiterals(remote, mit.getDateWith(2009, i), "mit");
//			
//			System.out.println(localPath);
//			System.out.println(remotePath);
//					
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(localPath);
//			tempDataRequest.setRemotePath(remotePath);
//			
//			
//			mit.submitDataRequest(tempDataRequest);
//		}
//		
//		mit.connect();
//		mit.putToSleep(240);
//		mit.disconnect();
//		String stationName = "p113";
//		for (int i = 1; i < 366; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.08d.Z";
//			remote = "/pub/rinex/obs/2008/"+ddd+"/"+stationName+ddd+"0.08d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			unavco.submitDataRequest(tempDataRequest);
//		}
//		
//		String stationName = "wuhn";
//		for (int i = 1; i < 365; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.07d.Z";
//			remote = "/pub/rinex/obs/2007/"+ddd+"/"+stationName+ddd+"0.07d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			unavco.submitDataRequest(tempDataRequest);
//		}
//		
//		stationName = "albh";
//		for (int i = 1; i < 366; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.08d.Z";
//			remote = "/pub/rinex/obs/2008/"+ddd+"/"+stationName+ddd+"0.08d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			unavco.submitDataRequest(tempDataRequest);
//		}
//		
//		stationName = "albh";
//		for (int i = 1; i < 365; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.07d.Z";
//			remote = "/pub/rinex/obs/2007/"+ddd+"/"+stationName+ddd+"0.07d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			unavco.submitDataRequest(tempDataRequest);		}
//		
//		stationName = "algo";
//		
//		for (int i = 1; i < 366; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.08d.Z";
//			remote = "/pub/rinex/2008/"+ddd+"/"+stationName+ddd+"0.08d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			sopac.submitDataRequest(tempDataRequest);		}
//		
//		
//		stationName = "algo";
//		
//		for (int i = 1; i < 365; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.07d.Z";
//			remote = "/pub/rinex/2007/"+ddd+"/"+stationName+ddd+"0.07d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			sopac.submitDataRequest(tempDataRequest);		
//		}
//		
//		stationName = "thu3";
//		for (int i = 1; i < 366; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.08d.Z";
//			remote = "/pub/rinex/2008/"+ddd+"/"+stationName+ddd+"0.08d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			sopac.submitDataRequest(tempDataRequest);		}
//		
//		stationName = "thu3";
//		
//		for (int i = 1; i < 365; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.07d.Z";
//			remote = "/pub/rinex/2007/"+ddd+"/"+stationName+ddd+"0.07d.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			sopac.submitDataRequest(tempDataRequest);		}
//		
//		stationName = "wuhn";
//		
//		for (int i = 1; i < 366; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.08o.Z";
//			remote = "/gps/data/daily/2008/"+ddd+"/08o/"+stationName+ddd+"0.08o.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			cddis.submitDataRequest(tempDataRequest);		}
//		
//		stationName = "wuhn";
//		
//		for (int i = 1; i < 365; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.07o.Z";
//			remote = "/gps/data/daily/2007/"+ddd+"/07o/"+stationName+ddd+"0.07o.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			cddis.submitDataRequest(tempDataRequest);		}
//		
//		stationName = "alrt";
//		
//		for (int i = 1; i < 366; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.08o.Z";
//			remote = "/gps/data/daily/2008/"+ddd+"/08o/"+stationName+ddd+"0.08o.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			cddis.submitDataRequest(tempDataRequest);		}
//		
//		stationName = "alrt";
//		
//		for (int i = 1; i < 365; i=i+1){
//			
//			ddd = Integer.toString(i);
//			
//			if (i < 10){
//				ddd = "00"+ddd;
//			}else if (i > 9 && i < 100){
//				ddd = "0"+ddd;
//			}
//			
//			local  = "./data/"+stationName+"/"+stationName+ddd+"0.07o.Z";
//			remote = "/gps/data/daily/2007/"+ddd+"/07o/"+stationName+ddd+"0.07o.Z";
//			
//			tempDataRequest = dataRequest.copy();
//			tempDataRequest.setLocalPath(local);
//			tempDataRequest.setRemotePath(remote);
//			
//			
//			cddis.submitDataRequest(tempDataRequest);		
//		}
////		
////		sopac.connect();
//		unavco.connect();
////		cddis.connect();
////		
////		cddis.putToSleep(120);
//		unavco.putToSleep(120);
////		
////		sopac.disconnect();
//		unavco.disconnect();
////		cddis.disconnect();
//		
//		unavco.putToSleep(1);
		
//		mit.connect();
//		mit.putToSleep(60*20);
//		System.exit(0);
		
	}
}




