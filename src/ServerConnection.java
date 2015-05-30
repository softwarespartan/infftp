
// Blocking queue definitions
import java.io.File;
import java.io.IOException;
import java.util.Timer;
//import java.util.TimerTask;
//import java.util.TooManyListenersException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.enterprisedt.net.ftp.FileTransferClient;
import com.enterprisedt.net.ftp.FTPException;
import com.enterprisedt.net.ftp.FTPTransferType;

import java.util.Random;

// FTP connection definitions
//import ipworks.*;
// java IO exception definitions
//import java.io.IOException;


/*
 * Producer-Consumer pattern 
 * 
 * ServerConnection is the consumer of dataRequests sent to a server. 
 * 
 */

public class ServerConnection extends Thread  {

	private static final Semaphore commandLock             = new Semaphore(1);
	private static final Timer     commandLockReleaseTimer = new Timer();
	
	// the connection to the ftp server
	private FileTransferClient connection;

	// feed off this queue ...block if empty
	private final BlockingQueue<DataRequest> dataRequestQueue;

	private RoutingServer routingServer = null;

	private Boolean shouldQuit = false;
	
	private Random randomGenerator = new Random();
	
	// AWS connection/session
    private final AmazonS3 s3;

	public ServerConnection(String serverAddress, 
			String userName, String password, 
			BlockingQueue<DataRequest> dataRequestQueue) throws IOException{

		
		this.s3 = new AmazonS3Client(
	            new PropertiesCredentials(
	                    ServerConnection.class
	                    	.getResourceAsStream("AwsCredentials.properties")
	            )
	    );
		// once set .. it's final
		this.dataRequestQueue = dataRequestQueue;

		// initialize new ftp connection
		this.connection = new FileTransferClient();

		try{

			this.connection.setRemoteHost(serverAddress);
			this.connection.setUserName(userName);
			this.connection.setPassword(password);
 
			this.connection.setRemotePort(21);
			this.connection.setContentType(FTPTransferType.BINARY);

		} catch (FTPException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ServerConnection(String serverAddress,int port, 
			String userName, String password, 
			BlockingQueue<DataRequest> dataRequestQueue)throws IOException{

		
		this.s3 = new AmazonS3Client(
	            new PropertiesCredentials(
	                    ServerConnection.class
	                    	.getResourceAsStream("AwsCredentials.properties")
	            )
	    );

		
		// once set .. it's final
		this.dataRequestQueue = dataRequestQueue;

		// initialize new ftp connection
		this.connection = new FileTransferClient();

		try{

			this.connection.setRemoteHost(serverAddress);
			this.connection.setUserName(userName);
			this.connection.setPassword(password);
 
			this.connection.setRemotePort(port);
			this.connection.setContentType(FTPTransferType.BINARY);;


		} catch (FTPException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	public ServerConnection(String serverAddress,int port, 
							String userName, String password, 
							BlockingQueue<DataRequest> dataRequestQueue,
							RoutingServer routingServer)throws IOException{

		
		this.s3 = new AmazonS3Client(
	            new PropertiesCredentials(
	                    ServerConnection.class
	                    	.getResourceAsStream("AwsCredentials.properties")
	            )
	    );
		
		this.routingServer = routingServer;
		
		// once set .. it's final
		this.dataRequestQueue = dataRequestQueue;

		// initialize new ftp connection
		this.connection = new FileTransferClient();

		try{

			this.connection.setRemoteHost(serverAddress);
			this.connection.setUserName(userName);
			this.connection.setPassword(password);
 
			this.connection.setRemotePort(21);
			this.connection.setContentType(FTPTransferType.BINARY);;


		} catch (FTPException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private synchronized void connect() throws FTPException, IOException {
		this.connection.connect();
	}

	private void disconnect() throws FTPException, IOException{
		this.connection.disconnect();
	}

	private synchronized Boolean initializeLocalPathOnFileSystem(String path){

		// assume true
		Boolean couldMakeLocalDir = true;
		Boolean canWriteToLocalDir = true;

		// get the parent path for the local file
		// i.e. the dir that will contain the file we download here
		File localFile = new File(path).getParentFile();

		// check that the parent dir actually exists
		if(!localFile.exists()){
			
			// No?  Well then make it ....
			couldMakeLocalDir = localFile.mkdirs();
			return couldMakeLocalDir;
		}

		// if it existed need to check that we can write to it
		canWriteToLocalDir = localFile.canWrite();

		// if we can not wirte to parent dir then dont bother to get the file
		if (!couldMakeLocalDir || !canWriteToLocalDir ){
			return false;
		}else{
			// all's good in the hood ... do the damn thing!
			return true;
		}
	}


	public void run() {

		try {
			this.connect();
		} catch (FTPException e1) {
			e1.printStackTrace();
			return;
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}


		// loop forever ... and ever
		while (true){
			try {

				//System.out.println(this.getName()+": number of waiting data requests in server queue: "+this.dataRequestQueue.size());
				execute(this.dataRequestQueue.take());

			} catch (InterruptedException e) {

				// it's been fun but ...
				
				System.out.println(this.getName()+": Received interrupt!");

				// release server connection
				//this.disconnect();

				// exit thread
				return;
				
			} catch (Exception ee){
				System.out.println("caught exception e = "+ee);
			}
		}
	}

	private void execute(final DataRequest request) { 
		
		if ( new File(request.getLocalPath()).exists()) return;

		// check initialization status
		if (! this.initializeLocalPathOnFileSystem(request.getLocalPath())){
			System.err.println(
					this.getName()
					+": Could not make local path: "
					+request.getLocalPath()
			);
			this.routingServer.submitDataRequest(request);
			return;
		}
		
		try{
			
			if (!this.connection.isConnected()) this.connect();
			
			this.connection.downloadFile(request.getLocalPath(), request.getRemotePath());
			System.out.println(this.getName()
				    +" - "+connection.getRemoteHost()
					+": "+request.getRemotePath()
					+" --> "+request.getLocalPath()
			);

			this.pushS3(request);

		} catch (FTPException e){

			if(this.routingServer != null)
				this.routingServer.submitDataRequest(request);
			return;
		}catch (AmazonS3Exception s3e){
			s3e.printStackTrace();		
		} catch (Exception e){
			e.printStackTrace();
		} 
	}
	
	private class S3Def{
		String bucket;
		String key   ;
		S3Def (String b,String k) {this.bucket = b; this.key = k;}
		public String toString(){return this.bucket+":"+this.key;}
	}
	
	protected S3Def computeS3Def(DataRequest dataRequest){
		
		// the as it sits on the local file system (just downloaded)
		File file = new File(dataRequest.getLocalPath());
		
		// the type of data product (typically, sp3, nav, rnx, etc)
		String dataType = dataRequest.getDataProduct();
		
		
		if (dataType.equalsIgnoreCase("rinex")){
			// os specific ...   :(
			final String pathSeparator = "/";
			
	        // split the file into parts based on os path separator
	        String[] fileParts = file.getAbsolutePath().split(pathSeparator);
	        
	        // make sure have valid archive path
	        if (fileParts.length != 11) {return null;}
	
	        // init file key
	        String fileKey  = fileParts[6]
	                        + pathSeparator
	                        + fileParts[8]
	                        + pathSeparator
	                        + fileParts[9]
	                        + pathSeparator
	                        + fileParts[10];
	
	        return new S3Def("rinex",fileKey);
	        
		} else if (dataType.startsWith("sp3")){
			return new S3Def("com.widelane.sp3",file.getName());
		}else if (dataType.startsWith("nav")){
			return new S3Def("com.widelane.nav",file.getName());
		} else {
			return null;
		}
    }

    public boolean fileExists(S3Def def)
            throws AmazonClientException
    {
        // init flag
        boolean isValidFile = true;

        try {

            // pull the metadata
            this.s3.getObjectMetadata(def.bucket, def.key);

        } catch (AmazonS3Exception s3e) {

            // no such file
            if (s3e.getStatusCode() == 404) {

                // i.e. 404: NoSuchKey
                isValidFile = false;
            }

            // don't swallow other exception
            else {

                // rethrow all S3 exceptions other than 404
                throw s3e;
            }
        }

        return isValidFile;
    }
    
    public void pushS3(DataRequest dataRequest) throws AmazonS3Exception {
    	
    	File file = new File(dataRequest.getLocalPath());
    	
    	// figure out where to put this file in S3
    	S3Def def = this.computeS3Def(dataRequest);
    	
    	// make sure the computation was successful
    	if (def == null) {return;}
    	
    	 // file transfer to S3
    	System.out.println(this.getName() + " - " + file.getPath() + " --> " + def);

        // do it!
        this.s3.putObject(def.bucket, def.key, file);
    }
    
    
}





