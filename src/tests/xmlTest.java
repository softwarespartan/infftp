package tests;

import java.util.Stack;
import java.util.TooManyListenersException;

import ipworks.*;

public class xmlTest implements XmlpEventListener {

	public Xmlp xmlp = new Xmlp();
	public String xPath = "";
	public Stack<String> xPathStack = new Stack<String>();
	public String currentElement= "/";
	public int numberOfServers = 0;
	
	
	xmlTest(){
		try {
			try {
				xmlp.setValidate(false);
			} catch (IPWorksException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			xmlp.addXmlpEventListener(this);
		} catch (TooManyListenersException e) {
			e.printStackTrace();
		}
	}
	
	public String getXPath(){
		
		String path = "";
		
		//make copy of xPathStack
		Object[] elements = this.xPathStack.toArray();
				
		for (int i=0;i<elements.length;i= i+1){
			path = path+"/"+elements[i];
		}
		return path;
	}

	@Override
	public void PI(XmlpPIEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void characters(XmlpCharactersEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void comment(XmlpCommentEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startElement(XmlpStartElementEvent event) {
		//System.out.println("Start Element Name: "+event.element);
		this.xPathStack.push(event.element);
		System.out.println("Start Element xPath: "+this.getXPath());
		
		if (event.element.equals("server")){
			this.numberOfServers+=1;
		}
		
	}
	
	@Override
	public void endElement(XmlpEndElementEvent event) {
		//System.out.println("End Element Name: "+event.element);
		this.xPathStack.pop();
		System.out.println("End Element xPath  : "+this.getXPath());
	}

	@Override
	public void endPrefixMapping(XmlpEndPrefixMappingEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void error(XmlpErrorEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void evalEntity(XmlpEvalEntityEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ignorableWhitespace(XmlpIgnorableWhitespaceEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void meta(XmlpMetaEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void specialSection(XmlpSpecialSectionEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startPrefixMapping(XmlpStartPrefixMappingEvent event) {
		
	}
	
	public static void main(String args[]){		
		
		xmlTest tmpXML = new xmlTest();
		try {
			
			tmpXML.xmlp.parseFile("server.info");
			System.out.println("Number of Servers: "+tmpXML.numberOfServers);
			
			String serverName;
			for (int i = 1; i<=tmpXML.numberOfServers+1; i+=1){
				
				try{
				tmpXML.xmlp.setXPath("/root/serverList/server["+i+"]/name");
				serverName = tmpXML.xmlp.getXText();
				System.out.print(serverName+", ");
				
				tmpXML.xmlp.setXPath("/root/serverList/server["+i+"]/userName");
				System.out.print(tmpXML.xmlp.getXText()+", ");
				
				tmpXML.xmlp.setXPath("/root/serverList/server["+i+"]/password");
				System.out.print(tmpXML.xmlp.getXText()+", ");
				
				tmpXML.xmlp.setXPath("/root/serverList/server["+i+"]/numberOfThreads");
				System.out.print(tmpXML.xmlp.getXText());
				
				System.out.println();
				
				for (int j = 1; j<100000;j+=1){
					try{
						tmpXML.xmlp.setXPath("/root/serverList/server["+i+"]/dataProduct["+j+"]/name");
						System.out.print(serverName+" Data Product: "+tmpXML.xmlp.getXText());
						
						tmpXML.xmlp.setXPath("/root/serverList/server["+i+"]/dataProduct["+j+"]/path");
						System.out.println(" @ "+tmpXML.xmlp.getXText());
						
					} catch (IPWorksException e){
						break;
					}
				}
				
				}catch (IPWorksException e){
					break;
				}
				
				
			}
		} catch (IPWorksException e) {
			e.printStackTrace();
		} 
	}
	


}