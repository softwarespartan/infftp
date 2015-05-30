import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TooManyListenersException;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import ipworks.IPWorksException;
import ipworks.Xmlp;
import ipworks.XmlpCharactersEvent;
import ipworks.XmlpCommentEvent;
import ipworks.XmlpEndElementEvent;
import ipworks.XmlpEndPrefixMappingEvent;
import ipworks.XmlpErrorEvent;
import ipworks.XmlpEvalEntityEvent;
import ipworks.XmlpEventListener;
import ipworks.XmlpIgnorableWhitespaceEvent;
import ipworks.XmlpMetaEvent;
import ipworks.XmlpPIEvent;
import ipworks.XmlpSpecialSectionEvent;
import ipworks.XmlpStartElementEvent;
import ipworks.XmlpStartPrefixMappingEvent;


public class DataRequestStringParser implements XmlpEventListener {

	private final Xmlp xmlParser = new Xmlp();
	//private final String xmlFile;
	public String xmlData;
	private int numberOfDataRequests = 0;
	private int numberOfKeys = 0;
	private String xPath;
	private final RoutingServer routingServer;
	
	// used to lock down the local path 
	private DateKeyPathParser dateKeyPathParser = new DateKeyPathParser();

	public DataRequestStringParser(RoutingServer routingServer){
		
		// set up the routing service
		this.routingServer = routingServer;

		try {

			// tell the parser not to validate the input
			try {
				this.xmlParser.setValidate(false);
				this.xmlParser.setRuntimeLicense(IPWorksLicense.IPWorksLicense);
			} catch (IPWorksException e) {
				e.printStackTrace();
			}

			// add ourselves to be notified of events
			this.xmlParser.addXmlpEventListener(this);

		} catch (TooManyListenersException e) {
			e.printStackTrace();
		}
	}

	public void parse(String xmlData) throws IPWorksException{
		this.xmlParser.input(xmlData);
	}
	
	public void setDateKeyPathParser(DateKeyPathParser dateKeyPathParser) {
		this.dateKeyPathParser = dateKeyPathParser;
	}

	public DateKeyPathParser getDateKeyPathParser() {
		return dateKeyPathParser;
	}

	public String parseDataRequests(){
		return this.inflateDataRequests(this.getDataRequestTemplates());
	}

	public Set<DataRequest> getDataRequestTemplates(){

		// initialize new set of data requests
		Set<DataRequest> dataRequestsSet = new HashSet<DataRequest>(this.numberOfDataRequests);
		
		
		String destination;
		String exclusiveServer;
		String dataProductName;
		
		String priorityServerName;
		String excludedServerName;
		String key;
		
		for (int i = 1; i <= this.numberOfDataRequests; i += 1){
			
			// empty data request
			DataRequest dataRequest = new DataRequest();

			try{

				// first try to get destination.  if this fails then no point to parse the rest
				this.xPath = "/RequestDefinition/dataRequestList/dataRequest["+i+"]/destination";
				destination = this.getElementValue(this.xPath);
				dataRequest.setLocalPathTemplate(destination);
				
				//System.out.println("DEST: "+destination);
				
				// next try to get the name of the desired data product
				// notice here that i'll just get the first one.  duplicates are ignored (for now ...)
				this.xPath = "/RequestDefinition/dataRequestList/dataRequest["+i+"]/dataProduct[1]";
				dataProductName = this.getElementValue(this.xPath);
				dataRequest.setDataProduct(dataProductName);
				
				//System.out.println("DP: "+dataProductName);
								
				// see if the data request is server exclusive
				try{
					// if this throws an exception then it doesn't exist in the file
					this.xPath = "/RequestDefinition/dataRequestList/dataRequest["+i+"]/serverExclusive";
					exclusiveServer = this.getElementValue(this.xPath);
					
					// not in the catch so must be listed
					dataRequest.setExclusiveServerName(exclusiveServer);
					dataRequest.setServerExclusive();
					
					//System.out.println("Setting server Exclusive: "+exclusiveServer+" is server exclusive: "+ dataRequest.isServerExclusive());
					
					//System.out.println("ES: "+exclusiveServer);
					
				} catch (IPWorksException e){
					//do nothing
				}
				
				// now try to process server priorities ...
				int j = 1;
				while(true){
					
					try{
						this.xPath = "/RequestDefinition/dataRequestList/dataRequest["+i+"]/serverPriority["+j+"]";
						priorityServerName = this.getElementValue(this.xPath);
						dataRequest.addPriorityServer(priorityServerName);
						
						//System.out.println("SP: "+priorityServerName);
						
						j +=1;
					} catch (IPWorksException e){
						// no more/ any priority servers
						break;
					}
				}
				
				// try to parse any excluded servers
				j = 1;
				while(true){
					
					try{
						this.xPath = "/RequestDefinition/dataRequestList/dataRequest["+i+"]/serverExcludes["+j+"]";
						excludedServerName = this.getElementValue(this.xPath);
						dataRequest.addPriorityServer(excludedServerName);
						
						//System.out.println("E: "+excludedServerName);
						
						j += 1;
					} catch (IPWorksException e){
						//e.printStackTrace();
						// no more/ any priority servers
						break;
					}
				}
				
				// finally, try to parse keys
				j = 1;
				while(true){
					
					try{
						
						// try to get a key
						this.xPath = "/RequestDefinition/dataRequestList/dataRequest["+i+"]/key["+j+"]";
						key = this.getElementValue(this.xPath);
						
						//System.out.println(key);
						
						// if got one then make new copy of current data request template and
						// set it's key to current key
						DataRequest newDataRequest = dataRequest.copy();
						newDataRequest.setKey(key);
						
						// add to data Rquest set.
						dataRequestsSet.add(newDataRequest);
										
						j += 1;
						
					} catch (IPWorksException e){
						// no more/ any priority servers
						break;
					}
				}

			}catch(IPWorksException e){
				// error constructing the server just return nothing
				System.out.println("ERROR parsing xml data ... ");
				System.out.println("Current this.xPath: "+ this.xPath);
				e.printStackTrace();
				continue;
			}

		}

//		try {
//			this.xmlParser.reset();
//		} catch (IPWorksException e) {
//			e.printStackTrace();
//		}
		return dataRequestsSet;
	}

	private String getElementValue(){
		// oust any white spaces
		return this.xmlParser.getXText().replaceAll("^\\s+", "");
	}

	private String getElementValue(String xPath) throws IPWorksException{
		this.setElementPath(xPath);
		return this.getElementValue();
	}

	private void setElementPath(String path) throws IPWorksException{
		this.xmlParser.setXPath(path);
	}
	
	private DateTime getStartDate() {
		
		try{
		
			//try to parse  star year
			this.xPath = "/RequestDefinition/startDate/year";
			String startYear = this.getElementValue(this.xPath);
			
			// try to parse start day of year
			this.xPath = "/RequestDefinition/startDate/doy";
			String startDoy  = this.getElementValue(this.xPath);
			
			// that's all folks
			return getDateWith(Integer.parseInt(startYear),
					           Integer.parseInt(startDoy));
			
		} catch (IPWorksException e){
			return null;
		}
	}
	
	private DateTime getStopDate() {
		
		try{
		
			//try to parse  star year
			this.xPath = "/RequestDefinition/stopDate/year";
			String stopYear = this.getElementValue(this.xPath);
			
			// try to parse start day of year
			this.xPath = "/RequestDefinition/stopDate/doy";
			String stopDoy  = this.getElementValue(this.xPath);
			
			// that's all folks
			return getDateWith(Integer.parseInt(stopYear),
							   Integer.parseInt(stopDoy));
			
		} catch (IPWorksException e){
			return null;
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
	
	public String inflateDataRequests(Set<DataRequest> dataRequestSet){
		
		Iterator<DataRequest> iter = dataRequestSet.iterator();

		
		// the goal here is to expand substitutions 
		// so if start data and stop data are found 
		// then each data request template needs to be expanded 
		// to cover these dates
		
		String absoluteLocalPath;
		
		DateTime startDate = this.getStartDate();
		DateTime stopDate  = this.getStopDate();
		
		try {
			this.xmlParser.reset();
		} catch (IPWorksException e) {
			e.printStackTrace();
		}
		
		// the base case is that either start date or stop date is null
		// in this instance, inflated data set is equal to the original 
		// dataRequest set
		if (this.routingServer != null && ( startDate == null || stopDate == null)){
			// nothing to inflate
			while(iter.hasNext()){
				this.routingServer.submitDataRequest(iter.next());
			}
			return null;
		}
		
		
		
		// calculate the number of days between start and stop date
		int numDays = Days.daysBetween(startDate, stopDate).getDays();
			
		DataRequest tmpDataRequest;
		
		int i= 0;
		// loop 
		while(iter.hasNext()){
			
			tmpDataRequest = iter.next();
			
			// loop between start and stop dates
			for (int j = 0; j <= numDays; j+=1){
				
				// make new data request from template that has effective date set;
				DataRequest newDataRequest = tmpDataRequest.copy();
				
				// set new date
				newDataRequest.setEffectiveDate(startDate.plusDays(j));
				
				// update local path with date info
				//newDataRequest.applyDateToLocalPath();
				absoluteLocalPath = this.dateKeyPathParser.replaceWithLiterals(newDataRequest.getLocalPathTemplate(),
																			   startDate.plusDays(j), 
																			   newDataRequest.getKey());
				
				newDataRequest.setLocalPathTemplate(absoluteLocalPath);
				
				//newDataRequest.print();

				
				i+=1;
				
				// hand off to the routing server
				if (this.routingServer != null){
					//newDataRequest.print();
					this.routingServer.submitDataRequest(newDataRequest);	
				}else{
					//System.out.println(i);
				}
			}
		}	
		
		DateTimeFormatter fmt = DateTimeFormat.forPattern("y::D");
		return "Start date: "+ fmt.print(startDate)
	 	   + ", Stop date: " + fmt.print(stopDate)
	 	   + ", Number of days: "+numDays + "\n"
	 	   +"From templates "+dataRequestSet.size()
	 	   +", inflated "+i+" data requests ...";
	}
	
	public void print(Set<DataRequest> dataRequestsSet){
		Iterator<DataRequest> iter = dataRequestsSet.iterator(); 
		while (iter.hasNext()){
			iter.next().print();
		}
	}

	@Override
	public void PI(XmlpPIEvent event) {		
	}

	@Override
	public void characters(XmlpCharactersEvent event) {		
	}

	@Override
	public void comment(XmlpCommentEvent event) {		
	}

	@Override
	public void endElement(XmlpEndElementEvent event) {		
	}

	@Override
	public void endPrefixMapping(XmlpEndPrefixMappingEvent event) {		
	}

	@Override
	public void error(XmlpErrorEvent event) {		
	}

	@Override
	public void evalEntity(XmlpEvalEntityEvent event) {		
	}

	@Override
	public void ignorableWhitespace(XmlpIgnorableWhitespaceEvent event) {		
	}

	@Override
	public void meta(XmlpMetaEvent event) {		
	}

	@Override
	public void specialSection(XmlpSpecialSectionEvent event) {		
	}

	@Override
	public void startElement(XmlpStartElementEvent event) {	
		if(event.element.equals("dataRequest")){
			this.numberOfDataRequests +=1;
		}else if (event.element.equals("key")){
			this.numberOfKeys += 1;
		}
		
		if (event.element.equals("RequestDefinition")){
			this.numberOfDataRequests = 0;
			this.numberOfKeys = 0;
		}
	}

	@Override
	public void startPrefixMapping(XmlpStartPrefixMappingEvent event) {		
	}
	
	public static String readFileAsString(String filePath) throws java.io.IOException{
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(
                new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            fileData.append(buf, 0, numRead);
        }
        reader.close();
        return fileData.toString();
    }
	
	public void parseFile(String file){
		String xmlData;
		try {
			xmlData = readFileAsString(file);
			this.parse(xmlData);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (IPWorksException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IPWorksException, IOException {
				
		DataRequestStringParser drp = new DataRequestStringParser(null);
		//System.out.println(DataRequestStringParser.readFileAsString("dataRequestClk.xml"));
		drp.parseFile("dataRequestClk.xml");
		drp.parseDataRequests();
		
		String xmlData = DataRequestStringParser.readFileAsString("dataRequestOSUGlobal.xml");
		drp.parse(xmlData);
		drp.parseDataRequests();
	}
}
