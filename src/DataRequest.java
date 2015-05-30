
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.joda.time.DateTime;

public class DataRequest {

	// what the ftp connection needs to know
	private String localPath;
	private String localPathTemplate;
	private String remotePath;
	
	// server routing information
	private String        exclusiveServerName;
	private Boolean       serverExclusive;
	private Queue<String> prioityServersList;
	private Set<String>   excludedServersList;
	private Set<String>   visitedServerList;
	
	// remote path determination stuff
	// this is what the server needs to know
	private String key;
	private String dataProduct;
	private DateTime effectiveDate;

	public DataRequest(){

		this.localPath   = null;
		this.localPathTemplate = null;
		this.remotePath  = null;
		
		this.key           = null;
		this.dataProduct   = null;
		this.effectiveDate = null;
		
		this.serverExclusive     = false;
		this.exclusiveServerName = null;
		
		this.prioityServersList  = new LinkedList<String>();
		this.excludedServersList = new HashSet<String>();
		this.visitedServerList   = new HashSet<String>();

	}
	
	public DataRequest(String dataProduct, 
					   String key, 
					   String localPath, 
					   DateTime dt){
		
		this.localPath   = null;
		this.localPathTemplate = localPath;
		this.remotePath  = null;
		
		this.key           = key;
		this.dataProduct   = dataProduct;
		this.effectiveDate = dt;
		
		this.serverExclusive     = false;
		this.exclusiveServerName = null;
		
		this.prioityServersList  = new LinkedList<String>();
		this.excludedServersList = new HashSet<String>();
		this.visitedServerList   = new HashSet<String>();
		
	}
	
	public DataRequest copy(){
		
		// use base constructor to make new instance
		DataRequest newDataRequest = new DataRequest(this.getDataProduct(),
													 this.getKey(),
													 this.getLocalPathTemplate(),
													 this.getEffectiveDate());
		
		// set server exclusive
		newDataRequest.setServerExclusive(this.isServerExclusive());
		newDataRequest.setExclusiveServerName(this.getExclusiveServerName());
		
		// copy over priority server information
		Iterator<String> pIter = this.prioityServersList.iterator();
		while(pIter.hasNext()){
			newDataRequest.addPriorityServer(pIter.next());
		}
		
		// copy excluded servers
		Iterator<String> eIter = this.getExcludedServersList().iterator();
		while(eIter.hasNext()){
			newDataRequest.excludeServer(eIter.next());
		}
		
		// forward visited servers
		Iterator<String> vIter = this.getVisitedServerSet().iterator();
		while(vIter.hasNext()){
			newDataRequest.addVisitedServer(vIter.next());
		}
		
		// return the copy
		return newDataRequest;
	}
	
	public void addVisitedServer(String serverName){
		this.visitedServerList.add(serverName);
	}
	
	public Set<String> getVisitedServerSet(){
		return new HashSet<String>(this.visitedServerList);
	}

	public DateTime getEffectiveDate() {
		return new DateTime(effectiveDate);
	}
	
	public void setEffectiveDate(DateTime date){
		this.effectiveDate = date;
	}

	public void print(){
		System.out.println("KEY: "+this.key);
		System.out.println("DATA PRODUCT: "+this.dataProduct);
		
		if (this.effectiveDate != null){
			System.out.println("DATE: "+this.effectiveDate.getYear() + ":"+this.effectiveDate.getDayOfYear());
		}
		
		System.out.println("Local Path: "+this.localPath);
		System.out.println("Local Path Template: "+this.localPathTemplate);
		
	}
	
	public String getDataProduct() {
		return dataProduct;
	}
	
	public void setDataProduct(String dataProduct){
		this.dataProduct = dataProduct;
	}

	public String getKey() {
		return key;
	}
	
	public void setKey(String key){
		this.key = key;
	}

	public void addPriorityServer(String serverName){
		this.prioityServersList.add(serverName);
	}
	
	public void excludeServer(String serverName){
		this.excludedServersList.add(serverName);
	}
	
	public String getLocalPathTemplate(){
		return this.localPathTemplate;
	}
	
	public void setLocalPathTemplate(String path){
		this.localPathTemplate = path;
	}
	
	public String getPrioityServer() {
		return this.prioityServersList.poll();
	}

	public Set<String> getExcludedServersList() {
		return new HashSet<String>(excludedServersList);
	}

	public String getExclusiveServerName() {
		return exclusiveServerName;
	}
	
	public void setExclusiveServerName(String exclusiveServerName) {
		this.exclusiveServerName = exclusiveServerName;
	}

	public Boolean isServerExclusive(){
		return this.serverExclusive;
	}
	
	public void setServerExclusive(){
		this.serverExclusive = true;
	}
	
	public void setServerExclusive(Boolean b){
		this.serverExclusive = b;
	}

	public String getLocalPath() {
		return localPath;
	}
	
	public void setLocalPath(String localPath){
		this.localPath = localPath;
	}

	public String getRemotePath() {
		return this.remotePath;
	}
	
	public void setRemotePath(String remotePath){
		this.remotePath = remotePath;
	}

}
