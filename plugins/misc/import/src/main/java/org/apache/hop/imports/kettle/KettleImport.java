package org.apache.hop.imports.kettle;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.*;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.imports.HopImport;
import org.apache.hop.imports.IHopImport;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KettleImport extends HopImport implements IHopImport {

    public KettleImport(){
        super();
    }

    public KettleImport(String inputFolderName, String outputFolderName) {
        super(inputFolderName, outputFolderName);
    }

    @Override
    public void importHopFolder(){

        FilenameFilter kettleFilter = (dir, name) -> name.endsWith(".ktr") | name.endsWith("*.kjb");
        String[] kettleFileNames = inputFolder.list(kettleFilter);

        try {
            // Walk over all ktr and kjb files we received, migrate to hpl and hwf
            Stream<Path> kettleWalk = Files.walk(Paths.get(inputFolder.getAbsolutePath()));
            List<String> result = kettleWalk.map(x -> x.toString()).filter(f -> f.endsWith(".ktr") || f.endsWith(".kjb")).collect(Collectors.toList());
            result.forEach(kettleFilename -> {
                File kettleFile = new File(kettleFilename);
                importHopFile(kettleFile);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.logBasic("We found " + kettleFileNames.length + " kettle files. ");
    }

    @Override
    public void importHopFile(File kettleFile){

        try {
            Document doc = getDocFromFile(kettleFile);

            if(kettleFile.getName().endsWith(".ktr")){
                renameNode(doc, doc.getDocumentElement(), "pipeline");
            }else if(kettleFile.getName().endsWith(".kjb")){
                renameNode(doc, doc.getDocumentElement(), "workflow");
            }
            importDbConnectionsFromHopFile(doc);
            processNode(doc, doc.getDocumentElement());

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource domSource = new DOMSource(doc);
            String outFilename = kettleFile.getAbsolutePath().replaceAll(inputFolder.getAbsolutePath(), outputFolder.getAbsolutePath()).replaceAll(".ktr", ".hpl").replaceAll(".kjb", ".hwf");
            File outFile = new File(outFilename);
            String folderName = outFile.getParent();
            Files.createDirectories(Paths.get(folderName));
            StreamResult streamResult = new StreamResult(new File(outFilename));

            transformer.transform(domSource, streamResult);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void importXmlDbConn(String dbConnPath){
        Document doc = getDocFromFile(new File(dbConnPath));
        importDbConnectionsFromHopFile(doc);
    }

    public void importPropertiesDbConn(String dbConnPath){
        try{
            Properties properties = new Properties();
            File varFile = new File(dbConnPath);
            InputStream inputStream = new FileInputStream(varFile);
            properties.load(inputStream);
            List<String> connNamesList = new ArrayList<String>();
            Set connKeys = properties.keySet();
            connKeys.forEach(connKey -> {
                String connName = ((String)connKey).split("/")[0];
                if(!connNamesList.contains(connName)){
                    connNamesList.add(connName);
                };
            });

            connNamesList.forEach(connName -> {
                GenericDatabaseMeta database = new GenericDatabaseMeta();
                database.setDriverClass((String) properties.get(connName + "/driver"));
                database.setManualUrl((String) properties.get(connName + "/url"));
                database.setUsername((String) properties.get(connName + "/user"));
                database.setPassword((String) properties.get(connName + "/password"));
                IDatabase db = (IDatabase) database;
                DatabaseMeta databaseMeta = new DatabaseMeta();
                databaseMeta.setName(connName);
                db.setPluginId(databaseMeta.getPluginName());
                databaseMeta.setIDatabase(db);

                try {
                    databaseSerializer.save(databaseMeta);
                } catch (HopException e) {
                    e.printStackTrace();
                }
            });
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private void importDbConnectionsFromHopFile(Document doc){

        NodeList connectionList = doc.getElementsByTagName("connection");
        for(int i = 0; i < connectionList.getLength(); i++){
            if(connectionList.item(i).getParentNode().equals(doc.getDocumentElement())){
                Element connElement = (Element)connectionList.item(i);
                String databaseType = connElement.getElementsByTagName("type").item(0).getTextContent();
                List<IPlugin> databasePluginTypes = registry.getPlugins(DatabasePluginType.class);
                IPlugin databasePlugin = registry.findPluginWithId(DatabasePluginType.class, connElement.getElementsByTagName("type").item(0).getTextContent());

                try {
                    DatabaseMeta databaseMeta = new DatabaseMeta();
                    IDatabase iDatabase = (BaseDatabaseMeta) registry.loadClass(databasePlugin);

                    if(connElement.getElementsByTagName("name").getLength() > 0){
                        databaseMeta.setName(connElement.getElementsByTagName("name").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("server").getLength() > 0){
                        iDatabase.setHostname(connElement.getElementsByTagName("server").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("access").getLength() > 0){
                        iDatabase.setAccessType(DatabaseMeta.getAccessType(connElement.getElementsByTagName("access").item(0).getTextContent()));
                    }
                    if(connElement.getElementsByTagName("database").getLength() > 0){
                        iDatabase.setDatabaseName(connElement.getElementsByTagName("database").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("port").getLength() > 0){
                        iDatabase.setPort(connElement.getElementsByTagName("port").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("username").getLength() > 0) {
                        iDatabase.setUsername(connElement.getElementsByTagName("username").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("password").getLength() > 0){
                        iDatabase.setPassword(connElement.getElementsByTagName("password").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("servername").getLength() > 0){
                        iDatabase.setServername(connElement.getElementsByTagName("servername").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("tablespace").getLength() > 0){
                        iDatabase.setDataTablespace(connElement.getElementsByTagName("tablespace").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("data_tablespace").getLength() > 0){
                        iDatabase.setDataTablespace(connElement.getElementsByTagName("data_tablespace").item(0).getTextContent());
                    }
                    if(connElement.getElementsByTagName("index_tablespace").getLength() > 0){
                        iDatabase.setIndexTablespace(connElement.getElementsByTagName("index_tablespace").item(0).getTextContent());
                    }
                    Map<String, String> attributesMap = new HashMap<String, String>();
                    NodeList connNodeList = connElement.getElementsByTagName("attributes");
                    for(int j=0; j < connNodeList.getLength() ; j++){
                        if(connNodeList.item(j).getNodeName().equals("attributes")){
                            Node attributesNode = connNodeList.item(j);
                            for(int k=0; k < attributesNode.getChildNodes().getLength(); k++){
                                Node attributeNode = attributesNode.getChildNodes().item(k);
                                for(int l=0; l < attributeNode.getChildNodes().getLength(); l++){
                                    String code = "";
                                    String attribute = "";
                                    if(attributeNode.getChildNodes().item(l).getNodeName().equals("code")){
                                        code = attributeNode.getChildNodes().item(l).getTextContent();
                                    }
                                    if(attributeNode.getChildNodes().item(l).getNodeName().equals("attribute")){
                                        attribute = attributeNode.getChildNodes().item(l).getTextContent();
                                    }
                                    attributesMap.put(code, attribute);
                                }
                            }
                        }
                    }
                    iDatabase.setAttributes(attributesMap);
                    databaseMeta.setIDatabase(iDatabase);
                    iDatabase.setPluginId(databaseMeta.getPluginName());
//                    iDatabase.setPluginId(connElement.getElementsByTagName("type").item(0).getTextContent());
                    databaseMeta.setDatabaseType(connElement.getElementsByTagName("type").item(0).getTextContent());


                    // save every connection, effectively only saving the last connection with a given name.
                    // TODO: evaluate connections, offer choices to merge or optimize connections.
                    databaseSerializer.save(databaseMeta);
//                    log.logBasic("Saved connection '" + databaseMeta.getName() + "'");
                }catch (HopPluginException e) {
                    e.printStackTrace();
                }catch(HopException e){
                    e.printStackTrace();
                }catch(NullPointerException e){
                    log.logError("Exception processing connection: " + e.getMessage());
                    e.printStackTrace();
                }

            }
        }
    }

    private void renameNode(Document doc, Element element, String newElementName){
        doc.renameNode(element, null, newElementName);
    }


    private void processNode(Document doc, Node node){
        Node nodeToProcess = node;
        NodeList nodeList = nodeToProcess.getChildNodes();

        // do a first pass to remove repository definitions
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node repositoryNode = nodeList.item(i);
            if (repositoryNode.getNodeType() == Node.ELEMENT_NODE) {
                if (KettleConst.repositoryTypes.contains(repositoryNode.getTextContent())) {
                    nodeToProcess = processRepositoryNode(node);
                    nodeList = nodeToProcess.getChildNodes();
                }
            }
        }

        for (int i = 0; i < nodeList.getLength(); i++) {
            Node currentNode = nodeList.item(i);

            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {

                // remove superfluous elements
                if(KettleConst.kettleElementsToRemove.containsKey(currentNode.getNodeName())){
                    if(!StringUtils.isEmpty(KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()))){
                        if(currentNode.getParentNode().getNodeName().equals(KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()))){
                            currentNode.getParentNode().removeChild(currentNode);
                        }
                    }else{
                        currentNode.getParentNode().removeChild(currentNode);
                    }
                }

                // rename Kettle elements to Hop elements
                if(KettleConst.kettleElementReplacements.containsKey(currentNode.getNodeName())){
                    renameNode(doc, (Element)currentNode, KettleConst.kettleElementReplacements.get(currentNode.getNodeName()));
                }

                // replace element contents with Hop equivalent
                if(KettleConst.kettleReplaceContent.containsKey(currentNode.getTextContent())){
                    currentNode.setTextContent(KettleConst.kettleReplaceContent.get(currentNode.getTextContent()));
                }

                processNode(doc, currentNode);
            }

            // partial node content replacement
            if(currentNode.getNodeType() == Node.TEXT_NODE && !StringUtils.isEmpty(currentNode.getTextContent())){
                for(Map.Entry<String, String> entry : KettleConst.kettleReplaceInContent.entrySet()){
                    if(currentNode.getTextContent().contains(entry.getKey())){
                        currentNode.setTextContent(currentNode.getTextContent().replace(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }
    }

    private Node processRepositoryNode(Node repositoryNode){
        String filename = "";
        String directory = outputFolder.getAbsolutePath();
        String type = "";
        for(int i=0; i < repositoryNode.getChildNodes().getLength(); i++){
            Node childNode = repositoryNode.getChildNodes().item(i);
            if(childNode.getNodeName().equals("directory")){
                if(childNode.getTextContent().startsWith(System.getProperty("file.separator"))){
                    directory += childNode.getTextContent();
                }else{
                    directory += System.getProperty("file.separator") + childNode.getTextContent();
                }
                repositoryNode.removeChild(childNode);
            }
            if(childNode.getNodeName().equals("type")){
                if(KettleConst.jobTypes.contains(childNode.getTextContent())){
                    type = ".hwf";
                }
                if(KettleConst.transTypes.contains(childNode.getTextContent())){
                    type = ".hpl";
                }
            }
            if(childNode.getNodeName().equals("jobname") || childNode.getNodeName().equals("transname")){
                filename = childNode.getTextContent();
                repositoryNode.removeChild(childNode);
            }
            if(childNode.getNodeName().equals("filename")){
                filename = childNode.getTextContent().replaceAll(".ktr", "").replaceAll(".kjb", "");
//                childNode.setTextContent(directory + System.getProperty("file.separator") + filename + type);
                childNode.setTextContent(filename + type);
            }
            // hard coded local run configuration for now
            if(childNode.getNodeName().equals("run_configuration")){
                childNode.setTextContent("local");
            }
        }

/*
        for(int i=0; i < repositoryNode.getChildNodes().getLength(); i++){
            Node childNode = repositoryNode.getChildNodes().item(i);
            if(childNode.getNodeName().equals("filename")){
                childNode.setTextContent(directory + System.getProperty("file.separator") + filename + type);
            }
        }
*/

        return repositoryNode;
    }

    private Document getDocFromFile(File kettleFile){
        try{
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setIgnoringElementContentWhitespace(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(kettleFile);
            return doc;
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
