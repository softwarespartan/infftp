import java.util.HashMap;
import java.util.Stack;
import java.util.TooManyListenersException;

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


public class DataProductsParser implements XmlpEventListener {
	
	private final Xmlp xmlParser = new Xmlp();
	private final String xmlFile;
	private Stack<String> xPathStack = new Stack<String>();
	private Stack<String> xElementStack = new Stack<String>();
	private int numberOfServers = 0;
	
	private HashMap<String,HashMap<String,String>> dataProductDefinitions 
		= new HashMap<String,HashMap<String,String>>();

	
	public DataProductsParser(String fileToParse){
		
		// set the file to parse
		this.xmlFile = fileToParse;
		
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

	public void parse() throws IPWorksException{
		this.xmlParser.parseFile(this.xmlFile);
		this.constructDataProductDefinitions();
	}
	
	public HashMap<String,HashMap<String,String>> getDataProductDefinitions(){
		return this.dataProductDefinitions;
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
	
	private String getXPath(){
		String path = "/";
		Object[] elements = this.xElementStack.toArray();
		for (int i = 0; i < elements.length; i += 1){
			path = path + elements[i] + "/";
		}
		return path;
	}
	
	public int getNumberOfServers(){
		return this.numberOfServers;
	}
	
	public void constructDataProductDefinitions(){
		
		String dataProductName;
		String dataProductPath;
		String serverName;
		HashMap<String,String> dataProductDefinitions;
		
		for (int i = 1; i <= this.numberOfServers; i+= 1){
			
			try{
				
				serverName = this.getElementValue("/dataDefinitions/server["+i+"]/name");
				
				//System.out.println(serverName);
				
				// create the server object
				dataProductDefinitions = new HashMap<String,String>();
				
				int j = 1;
				while(true){
					
					try{
						//get data product information
						dataProductName = this.getElementValue("/dataDefinitions/server["+i+"]/dataProduct["+j+"]/name");
						dataProductPath = this.getElementValue("/dataDefinitions/server["+i+"]/dataProduct["+j+"]/path");
						
						//System.out.println(dataProductName + " @ "+ dataProductPath );
						
						// add to hash map 
						dataProductDefinitions.put(dataProductName, dataProductPath);
						
					} catch (IPWorksException e){
						break;
					}
					
					j+=1;
				}
				
				// add the data product set to data product definitions
				this.dataProductDefinitions.put(serverName,dataProductDefinitions);
				
			}catch(IPWorksException e){
				// error constructing the server just return nothing
				System.out.println("ERROR parsing data products in: "+this.xmlFile);
				continue;
			}
		}
	}
	
	public void printDataProductDefinitions(){
		HashMap<String, String> dataProductsDict;
		
		// get the set of servers
		Object[] serverList = this.dataProductDefinitions.keySet().toArray();
		
		// for each server
		for (int i = 0; i < serverList.length; i += 1){
			
			// print the name
			System.out.println("SERVER: "+serverList[i]);
			
			// get the data products for this server
			dataProductsDict = this.dataProductDefinitions.get(serverList[i]);
			
			// get the set of data products for this server
			Object[] dataProducts = dataProductsDict.keySet().toArray();			
			
			// print out all the current data products for this server
			for (int j = 0; j < dataProducts.length; j +=1){
				System.out.println("\t Data Product: "+dataProducts[j]+" @ "+dataProductsDict.get(dataProducts[j]));
			}
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
	public void startElement(XmlpStartElementEvent event) {	
		
		// first add the element to the element stack
		this.xElementStack.push(event.element);
		
		// ask for new xPath
		this.xPathStack.push(this.getXPath());
		
		// debug
		//System.out.println(this.getXPath());
		
		if (event.element.equalsIgnoreCase("server")){
			this.numberOfServers += 1;
		}
	}
	

	@Override
	public void endElement(XmlpEndElementEvent event) {	
		
		// remove element from stack to get
		// proper xPath next time
		this.xElementStack.pop();
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
	public void startPrefixMapping(XmlpStartPrefixMappingEvent event) {		
	}
	
	public static void main(String[] args) throws IPWorksException {
		
		DataProductsParser dpp = new DataProductsParser("dataProducts.xml");
		
		dpp.parse();
				
		dpp.printDataProductDefinitions();

	}

}
