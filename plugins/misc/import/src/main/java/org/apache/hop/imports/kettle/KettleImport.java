/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.imports.kettle;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.*;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.util.StringUtil;
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
import javax.xml.transform.dom.DOMSource;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KettleImport extends HopImport implements IHopImport {

    public int kjbCounter, ktrCounter, otherCounter = 0;

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
            kettleWalk = Files.walk(Paths.get(inputFolder.getAbsolutePath()));
            // TODO: add a proper way to exclude folders instead of hard coded .git exclude.
            List<String> otherFilesList = kettleWalk.map(x -> x.toString()).filter(f -> !f.endsWith(".ktr") && !f.endsWith(".kjb") && !f.contains(".git/")).collect(Collectors.toList());
            otherFilesList.forEach(otherFilename -> {
                File otherFile = new File(otherFilename);
                if(!otherFile.isDirectory()){
                    migratedFilesMap.put(otherFilename, null);
                    otherCounter++;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.logBasic("We found " + kettleFileNames.length + " kettle files. ");
    }

    @Override
    public void importHopFile(File kettleFile){

        Document doc = getDocFromFile(kettleFile);
        // import connections first
        importDbConnections(doc, kettleFile);

        // move to processNode?
        if(kettleFile.getName().endsWith(".ktr")){
            ktrCounter++;
            renameNode(doc, doc.getDocumentElement(), "pipeline");
        }else if(kettleFile.getName().endsWith(".kjb")){
            kjbCounter++;
            renameNode(doc, doc.getDocumentElement(), "workflow");
        }
        processNode(doc, doc.getDocumentElement());

        DOMSource domSource = new DOMSource(doc);
        String outFilename = "";

        if(System.getProperty("os.name").contains("Windows")){
            outFilename = kettleFile.getAbsolutePath().replaceAll("\\\\", "/")
                    .replaceAll(inputFolder.getAbsolutePath()
                            .replaceAll("\\\\", "/"), outputFolder.getAbsolutePath().replaceAll("\\\\", "/"))
                    .replaceAll(".ktr", ".hpl")
                    .replaceAll(".kjb", ".hwf");
        }else{
            outFilename = kettleFile.getAbsolutePath()
                    .replaceAll(inputFolder.getAbsolutePath(), outputFolder.getAbsolutePath())
                    .replaceAll(".ktr", ".hpl")
                    .replaceAll(".kjb", ".hwf");
        }
        migratedFilesMap.put(outFilename, domSource);

    }

    @Override
    public void importXmlDbConn(String dbConnPath){
        Document doc = getDocFromFile(new File(dbConnPath));
        importDbConnections(doc, null);
    }

    @Override
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

                addDatabaseMeta(varFile.getAbsolutePath(), databaseMeta);
            });
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private void importDbConnections(Document doc, File kettleFile){

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
                        databaseMeta.setName(getTextContent(connElement, "name", 0));
                    }
                    if(connElement.getElementsByTagName("server").getLength() > 0){
                        iDatabase.setHostname(getTextContent(connElement, "server", 0));
                    }
                    if(connElement.getElementsByTagName("access").getLength() > 0){
                        iDatabase.setAccessType(DatabaseMeta.getAccessType(getTextContent(connElement, "access", 0)));
                    }
                    if(connElement.getElementsByTagName("database").getLength() > 0){
                        iDatabase.setDatabaseName(getTextContent(connElement, "database", 0));
                    }
                    if(connElement.getElementsByTagName("port").getLength() > 0){
                        iDatabase.setPort(getTextContent(connElement, "port", 0));
                    }
                    if(connElement.getElementsByTagName("username").getLength() > 0) {
                        iDatabase.setUsername(getTextContent(connElement, "username", 0));
                    }
                    if(connElement.getElementsByTagName("password").getLength() > 0){
                        iDatabase.setPassword(getTextContent(connElement, "password",0 ));
                    }
                    if(connElement.getElementsByTagName("servername").getLength() > 0){
                        iDatabase.setServername(getTextContent(connElement, "servername", 0));
                    }
                    if(connElement.getElementsByTagName("tablespace").getLength() > 0){
                        iDatabase.setDataTablespace(getTextContent(connElement, "tablespace", 0));
                    }
                    if(connElement.getElementsByTagName("data_tablespace").getLength() > 0){
                        iDatabase.setDataTablespace(getTextContent(connElement, "data_tablespace", 0));
                    }
                    if(connElement.getElementsByTagName("index_tablespace").getLength() > 0){
                        iDatabase.setIndexTablespace(getTextContent(connElement, "index_tablespace", 0));
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
                    databaseMeta.setDatabaseType(connElement.getElementsByTagName("type").item(0).getTextContent());

                    addDatabaseMeta(kettleFile.getAbsolutePath(), databaseMeta);
                }catch (HopPluginException e) {
                    e.printStackTrace();
                }catch(NullPointerException e){
                    log.logError("Failed to parse connection of type '" + databaseType + "' for file '" + kettleFile.getAbsolutePath() + "'");
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

                    for(int j=0; j < node.getChildNodes().getLength(); j++){
                        Node childNode = node.getChildNodes().item(j);
                        if(childNode.getNodeName().equals("jobname") || childNode.getNodeName().equals("transname")){
                            if(!StringUtil.isEmpty(childNode.getTextContent())){
                                nodeToProcess = processRepositoryNode(node);
                            }
                        }
                    }
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
                        // see if we have multiple parent nodes to check for:
                        if(KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()).contains(",")){
                            Node parentNode = currentNode.getParentNode();
                            String[] parentNodenames = KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()).split(",");
                            for(String parentNodename : parentNodenames){
                                if(parentNode.getNodeName().equals(parentNodename)){
                                    parentNode.removeChild(currentNode);
                                }
                            }
                        }else {
                            if(currentNode.getParentNode().getNodeName().equals(KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()))){
                                currentNode.getParentNode().removeChild(currentNode);
                            }
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
        String directory = "${PROJECT_HOME}";
        String type = "";
        Node filenameNode = null;

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
            if(childNode.getNodeName().equals("filename")){
                filename = childNode.getTextContent().replaceAll(".ktr", "").replaceAll(".kjb", "");
                childNode.setTextContent(filename + type);
                filenameNode = childNode;
            }

            // hard coded local run configuration for now
            if(childNode.getNodeName().equals("run_configuration")){
                childNode.setTextContent("local");
            }
            if(childNode.getNodeName().equals("jobname") || childNode.getNodeName().equals("transname")){
                filename = childNode.getTextContent();
                repositoryNode.removeChild(childNode);
            }
        }

        filenameNode.setTextContent(directory + "/" + filename + type);

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

    private String getTextContent(Element element, String tagName, Integer itemIndex){
        return element.getElementsByTagName(tagName).item(itemIndex).getTextContent();
    }
}
