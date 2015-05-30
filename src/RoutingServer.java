import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;


public class RoutingServer extends Thread{
	
	
	// the set of servers to route to.
	// make concurrent so that server threads can add 
	// themselves in any order at any time
	// this relieves the initialization period/time
	private ConcurrentHashMap<String,Server> registeredServers;
	
	// data request are put here 
	private final BlockingQueue<DataRequest> dataRequestQueue;
	
	// keep track of routing server threads
	private final Set<RoutingServerWorkerThread> workerThreadSet 
		=  new HashSet<RoutingServerWorkerThread>(this.NUMBER_OF_WORKER_THREADS);
	
	// status timer so that we print status every N seconds instead 
	// every single state change ...
	private final Timer statusTimer;
	
	// well, the number of threads to start
	private final int NUMBER_OF_WORKER_THREADS = 2;

	public RoutingServer(){
		this.dataRequestQueue = new LinkedBlockingQueue<DataRequest>();
		this.registeredServers = new ConcurrentHashMap<String,Server>();
		statusTimer = new Timer();
	}
	
	public void addServer(Server server){
		this.registeredServers.put(server.getServerName(), server);
	}
	
	public void removeServer(Server server){
		this.registeredServers.remove(server.getServerName());
	}
	
	public synchronized void submitDataRequest(DataRequest dataRequest){
		this.dataRequestQueue.add(dataRequest);
	}
	
	public void initializeWorkerThreads(){
		
		for (int i = 1; i <= this.NUMBER_OF_WORKER_THREADS; i +=1){
			RoutingServerWorkerThread newThread 
				= new RoutingServerWorkerThread(this.registeredServers,this.dataRequestQueue);
			newThread.start();
			this.workerThreadSet.add(newThread);
		}
		
		int statusDelay = 10;
		
		// schedule the status task
		this.statusTimer.scheduleAtFixedRate(
											 new StatusTask(this.registeredServers), 
											 1*1000, 
											 statusDelay*1000);
	}
	
	public void shutDownWorkerThreads(){

		Iterator<RoutingServerWorkerThread> iter = this.workerThreadSet.iterator();
		while(iter.hasNext()){
			iter.next().interrupt();
		}
	}
	
	public void run(){

		
		// initialize the routing theads ...
		this.initializeWorkerThreads();

		while(true){
			
			// loop forever 
			if (this.isInterrupted()){
				this.shutDownWorkerThreads();
				return;
			}
		}
	}
	
	class StatusTask extends TimerTask {
		
		// the servers whos status you'll report on
		private final ConcurrentHashMap<String,Server> registeredServers;
		
		// general constructor to set the registered servers
		public StatusTask(ConcurrentHashMap<String,Server> registeredServers){
			
			// should call super?
			super();
			
			// set the servers
			this.registeredServers = registeredServers;
		}
		
	    public void run() {
	    	
			// get the set of keys for registered servers hash map ...
			Server server;
			String serverName;
			String stats = "";
			
			Iterator<String> keyIter = this.registeredServers.keySet().iterator();
			while(keyIter.hasNext()){
				
				serverName = keyIter.next();
				server = this.registeredServers.get(serverName);
				
				// for each server print the name of the server and number of jobs in it's queue
				stats += serverName + ": " +server.getJobCount()+", ";
			}
			
			System.out.println(stats);;
	    }
	}
	
	public String getNumberOfPendingJobsByServer(){
		
		// get the set of keys for registered servers hash map ...
		String serverName;
		String stats = "";
		
		Iterator<String> keyIter = this.registeredServers.keySet().iterator();
		while(keyIter.hasNext()){
			
			serverName = keyIter.next();
			
			// for each server print the name of the server and number of jobs in it's queue
			stats += serverName + ": " +this.registeredServers.get(serverName).getJobCount()+", ";
		}
		
		return stats;
	}
	
	public int getNumberOfPendingJobs(){
		
		int numJobs = 0;
		
		Iterator<String> keyIter = this.registeredServers.keySet().iterator();
		while(keyIter.hasNext()){
			
			// for each server print the name of the server and number of jobs in it's queue
			numJobs += this.registeredServers.get(keyIter.next()).getJobCount();
		}
		
		return numJobs;
	}
	
	public static void main(String[] args) {

	}

}
