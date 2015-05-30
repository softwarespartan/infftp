package tests;

import java.io.File;
import ipworks.*;

public class ipWorksTest2 {
	
	public static void main(String[] args) {
		
		// init ftp connection object
		final Ftp connection = new Ftp();
		
		try{
			// login info
			connection.setUser("anonymous");
			connection.setPassword("anonymous");
			connection.setRemoteHost("data-out.unavco.org");
			
			// misc settings
			connection.setTimeout(5000);
			connection.setPassive(true);
			connection.setTransferMode(Ftp.tmBinary);
			
			// est the connection
			connection.logon();
			
		}catch (IPWorksException e){
			e.printStackTrace();
		}
		
		//path variables
		String STATION_NAME = "wuhn";
		String ddd;
		
		// make local directory for data files if it does not exist already 
		File localDir = new File("./data/"+STATION_NAME+"/");
		if (!localDir.exists()){
			localDir.mkdirs();
		}
		
		ddd = "001";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "002";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "003";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "004";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "005";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "006";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "007";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "008";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "009";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
		ddd = "010";
		try{
			connection.setRemoteFile("/pub/rinex/obs/2007/"+ddd+"/"+STATION_NAME+ddd+"0.07d.Z");
			connection.setLocalFile("./data/"+STATION_NAME+"/"+STATION_NAME+ddd+"0.07d.Z");
			System.out.println("fulfilling data request: "+ connection.getRemoteFile()+ " from server "+connection.getRemoteHost());
			connection.download();
		}catch(IPWorksException e){
			e.printStackTrace();
		}
		
	}

}
