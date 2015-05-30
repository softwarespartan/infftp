package tests;

import ipworks.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ipWorksTest {
	
	
	class DataRequestObj{
		
		// strings to hold local and remote paths
		public final String localPath;
		public final String remotePath;
		
		// constructor
		public DataRequestObj(String remotePath,String localPath){
			this.localPath = localPath;
			this.remotePath = remotePath;
		}
	}

	/**
	 * test ipworks ftp API for missing files
	 */
	public static void main(String[] args) {
		
		// station "wuhn" does not exist for year 2007
		// station "p113" exists for every day of 2007
		final String STATION_NAME = "wuhn"; 
		
		// test object
		ipWorksTest testObj = new ipWorksTest();
		
		// init ftp connection object
		final Ftp connection = new Ftp();

		// configure the connection
		try {
			
			// login info
			connection.setUser("anonymous");
			connection.setPassword("anonymous");
			connection.setRemoteHost("data-out.unavco.org");
			
			// misc settings
			//connection.setTimeout(5000);
			//connection.setPassive(true);
			//connection.setTransferMode(Ftp.tmBinary);

		} catch (IPWorksException e) {
			e.printStackTrace();
		}
		
		// make a blocking queue and ...
		// i know this is over kill but this is what im using in 
		// real program with multiple ftp threads
		final BlockingQueue<DataRequestObj> dataRequestQueue 
			= new LinkedBlockingQueue<DataRequestObj>();
		
		// fill it up with data request objs
		// i.e. make a bunch of strings and 
		// make "datarequest" outta them ...
		String ddd,localPath,remotePath;
		DataRequestObj dataRequest;
		for (int i = 1; i < 365; i=i+1){
			
			ddd = Integer.toString(i);
			
			if (i < 10){
				ddd = "00"+ddd;
			}else if (i > 9 && i < 100){
				ddd = "0"+ddd;
			}
			
			// make final paths
			localPath  = "./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z";
			remotePath = "/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z";
			
			// construct data request outta them
			dataRequest = testObj.new DataRequestObj(remotePath,localPath);
			
			// queue up the request
			try {
				dataRequestQueue.put(dataRequest);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// now, connect to server
		try {
			connection.logon();
		} catch (IPWorksException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		
		// service each dataRequst in the queue
		DataRequestObj currentDataRequest;
		while(!dataRequestQueue.isEmpty()){
			
			try {
				// take a data request	
				currentDataRequest = dataRequestQueue.take();
				
				// configure the download
				connection.setLocalFile(currentDataRequest.localPath);
				connection.setRemoteFile(currentDataRequest.remotePath);
				
				// download the file 
				// if file DNE just catch exception and move on to next file 
				System.out.println(
								   "fulfilling data request: "
								   + currentDataRequest.remotePath
								   + " from server "
								   +connection.getRemoteHost()
								  );
				connection.download();

			} catch (IPWorksException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e){
				e.printStackTrace();
			}
		}
	}

}
