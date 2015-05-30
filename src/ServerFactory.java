
import java.util.HashMap;
import java.util.TooManyListenersException;
import ipworks.*;

public class ServerFactory implements XmlpEventListener {

	private Xmlp xmlParser = new Xmlp();
	private String xmlFile;
	private int numberOfServers = 0;
	private DataProductsParser dataProductsParser ;
	
	
	public ServerFactory(String fileToParse, String dataProductsFile){
		
		this.dataProductsParser = new DataProductsParser(dataProductsFile);
		
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
		this.dataProductsParser.parse();
	}
	
	private String getElementValue(){
		// trim any white spaces
		return this.xmlParser.getXText().replaceAll("^\\s+", "");
	}
	
	private String getElementValue(String xPath) throws IPWorksException{
		this.setElementPath(xPath);
		return this.getElementValue();
	}
	
	private void setElementPath(String path) throws IPWorksException{
		this.xmlParser.setXPath(path);
	}
	
	public int getNumberOfServers(){
		return this.numberOfServers;
	}
	
	public HashMap<String,Server> getServers(){
		
		Server server;
		HashMap<String,Server> serverSet = new HashMap<String,Server>(this.numberOfServers);
		
		for (int i = 1; i <= this.numberOfServers;i+=1){
			
			server = this.getServerAtIndx(i);
			if (server != null){
				serverSet.put(server.getServerName(), server);
			}
		}
		
		return serverSet;
	}
	
	private Server getServerAtIndx(int i){
		
		Server server = null;
		
		try{
			String serverName = this.getElementValue("/serverInfo/serverList/server["+i+"]/name");
			
			String serverAddress = this.getElementValue("/serverInfo/serverList/server["+i+"]/address");
			
			String userName = this.getElementValue("/serverInfo/serverList/server["+i+"]/userName");
			
			String password = this.getElementValue("/serverInfo/serverList/server["+i+"]/password");
			
			String numberOfThreads = this.getElementValue("/serverInfo/serverList/server["+i+"]/numberOfThreads");
			
			// try to get the port number if one exists
			String port = "21";
			try{
				port = this.getElementValue("/serverInfo/serverList/server["+i+"]/port");	
			} catch (IPWorksException e){
				// do nothing;
			}
			
			// create the server object
			server = new Server(serverName,
								serverAddress,
								userName,password,
								Integer.parseInt(port),
								Integer.parseInt(numberOfThreads));
			
		}catch(IPWorksException e){
			// error constructing the server just return nothing
			return null;
		}
		
		// finally add the data products for this server if there are any to add
		// otherwise return null since a server without any data products is just going to 
		// reject every job 
		if(this.dataProductsParser.getDataProductDefinitions().containsKey(server.getServerName())){
			server.setDataProducts(this.dataProductsParser.getDataProductDefinitions().get(server.getServerName()));
		}else{
			// no data product definitions so return nothing
			return null;
		}
		
		// at last, return the server configured object
		return server;
		
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
		// kkep track of the number of servers in the file
		if (event.element.equals("server")){
			this.numberOfServers+=1;
		}
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
	public void startPrefixMapping(XmlpStartPrefixMappingEvent event) {
	}
	
	public static void main(String args[]){		
		
		ServerFactory serverFactory = new ServerFactory("servers.xml","dataProducts.xml");
		
		try {
			serverFactory.parse();
		} catch (IPWorksException e) {
			e.printStackTrace();
			return;
		}
		
		for (int i = 1; i <= serverFactory.numberOfServers;i+=1){
			Server s = serverFactory.getServerAtIndx(i);
			
			if (s != null){
				s.print();
				System.out.println();
			}
		}
	}
}