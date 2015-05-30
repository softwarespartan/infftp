import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;


public class RoutingServerWorkerThread extends Thread {
	
	private final ConcurrentHashMap<String,Server> registeredServers;
	
	// data request are put here 
	private final BlockingQueue<DataRequest> dataRequestQueue;
	
	public RoutingServerWorkerThread(ConcurrentHashMap<String,Server> registeredServers,
									 BlockingQueue<DataRequest> dataRequestQueue){
		
		this.registeredServers = registeredServers;
		this.dataRequestQueue  = dataRequestQueue;
	}
	
	public void run(){
		
		while(true){
			try{
				this.routeDataRequest(this.dataRequestQueue.take());
			} catch (InterruptedException e){
				
				// time to shutdown thread and exit
				return;
			}
		}
	}
	
	private void routeDataRequest(DataRequest dataRequest){
		
		//dataRequest.print();			
//		Iterator<String> iter = dataRequest.getVisitedServerSet().iterator();
//		while(iter.hasNext()){
//			System.out.println("Visited SERVERS: "+iter.next()+ " " +dataRequest.getLocalPath());
//		}
		
//		// get the set of keys for registered servers hash map ...
//		Server server;
//		String stats = "";
//		
//		Iterator<String> keyIter = this.registeredServers.keySet().iterator();
//		while(keyIter.hasNext()){
//			
//			serverName = keyIter.next();
//			server = this.registeredServers.get(serverName);
//			
//			// for each server print the name of the server and number of jobs in it's queue
//			stats += serverName + ": " +server.getJobCount()+", ";
//		}
//		
//		System.out.println(stats);
		
		//System.out.println("trying to route jobs");
		
		// premero task is to see if the data request is server exclusive
		// if so, check if the request have been service by it's server
		// If the request has already visited it's exclusive server then
		// should not get rerouted. 
		if(dataRequest.isServerExclusive()){
			
			// first check if the request has already been to exclusive server
			if(! dataRequest.getVisitedServerSet().contains(dataRequest.getExclusiveServerName())){
				
				//System.out.println("Exclusive server has NOT been visited yet!");
				
				// check that exclusive server exists and if so route there
				if (this.registeredServers.containsKey(dataRequest.getExclusiveServerName())){
					
					// submit the job to exclusive server
					System.out.println(this.getName()+": Routing server exclusive job to server: "+dataRequest.getExclusiveServerName());

					this.registeredServers.get(dataRequest.getExclusiveServerName()).submitDataRequest(dataRequest);
					return;
				}
			}else{
				//System.out.println(this.getName()+": Can not reroute the job since it has aready visited exlcusive server!!!");
				//System.out.println(this.getName()+": COULD NOT REROUTE REQUEST: "+dataRequest.getLocalPath()+" - No server left to route to!!!");
				//System.out.println(this.getName()+": COULD NOT REROUTE REQUEST: "+dataRequest.getRemotePath()+" - No server left to route to!!!");
				return;
			}
		}
		
		// next, figure out if there is a priority server for this data request
		// if so, nothing left to figure out. 
		String priorityServer = dataRequest.getPrioityServer();
		
		// here if NOT null then got a server name
		// the question now is does the server actually exist?
		// if not loop through priority servers until one is found
		// if in the end we've tried all priority servers in the list and NONE of them are found
		// just proceed to default server scheduling!
		if(priorityServer != null){
			while(true){
				
				//check that priority server exists
				if (priorityServer == null){
					
					// no more server left to try
					break;
					
				}else if (this.registeredServers.containsKey(priorityServer)){
					// Alright! found a priority server that exists
					// submit the request to its preferred server
					//System.out.println(" DATA REQUEST: "+dataRequest.getLocalPath()+" to priority server: "+priorityServer);
					this.registeredServers.get(priorityServer).submitDataRequest(dataRequest);
					return;
				}else{
					
					// pick the next priority server if it exists
					priorityServer = dataRequest.getPrioityServer();
				}
			}
		}
		
		// if not, get list of all available servers (at the moment)
		Set<String> validServerList = new HashSet<String>(this.registeredServers.keySet());
				
		// step 1. remove all visited servers from valid server list
		validServerList.removeAll(dataRequest.getVisitedServerSet());
		
		// step 2. removal all excluded servers from the list
		validServerList.removeAll(dataRequest.getExcludedServersList());
		
		// step 3. if there are any servers left to route to then route to server
		//         with fewest requests queued
		//
		//  NOTE:  here this isnt optimal.  should probably look at something like
		//         number of requests serviced per second or something 
		//
		//         In the future will implement work stealing
		//
		if (! validServerList.isEmpty()){
			
			String serverName;
			
			int minTaskCount = 1000000000;
			int serverTaskCount;
			String targetServer = "";
			//String serverName;
			
			Iterator<String> rIter = validServerList.iterator();
			while(rIter.hasNext()){
				
				// get name of the server
				serverName = rIter.next();
				
				// get number of data requests queued by server
				serverTaskCount = this.registeredServers.get(serverName).getDataRequestQueue().size();
				
				// check server task count 
				if(serverTaskCount < minTaskCount){
					targetServer = serverName;
					minTaskCount = serverTaskCount;
				}
			}
			
			// finally route data request to this server
			//System.out.println(this.getName()+": ROUTING DATA REQUEST: "+dataRequest.getLocalPath()+" to server: "+targetServer);
			this.registeredServers.get(targetServer).submitDataRequest(dataRequest);
			
		} else {
			//System.out.println(this.getName()+": COULD NOT REROUTE REQUEST: "+dataRequest.getLocalPath()+" - No server left to route to!!!");
			dataRequest = null;
		}
	}

	public static void main(String[] args) {

	}

}
