/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.imports.kettle;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileFilterSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.filter.NameFileFilter;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlFormatter;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.imports.HopImport;
import org.apache.hop.imports.IHopImport;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KettleImport extends HopImport implements IHopImport {

  private int kjbCounter;
  private int ktrCounter;
  private int otherCounter;
  private String variablesTargetConfigFile;
  private String connectionsReportFileName;

  public KettleImport(IVariables variables) {
    super(variables);
  }

  @Override
  public void findFilesToImport() throws HopException {
    AtomicInteger count = new AtomicInteger();
    try {
      // Find all files...
      //

      List<FileObject> allFiles = new ArrayList<>();
      allFiles.addAll(HopVfs.findFiles(getInputFolder(), null, true));

      for (FileObject file : allFiles) {
        // Skip hidden files?
        //
        if (skippingHiddenFilesAndFolders && file.isHidden()) {
          continue;
        }
        String ext = file.getName().getExtension();
        if ("ktr".equalsIgnoreCase(ext) || "kjb".equalsIgnoreCase(ext)) {
          // This is a Kettle transformation or job
          //
          handleHopFile(file);
          count.incrementAndGet();
        } else {
          // Make sure it's not a folder or .git/ (redundant I know)
          //
          try {
            if (!file.getName().getURI().contains(".git/") && !file.isFolder()) {
              migratedFilesMap.put(file.getName().getURI(), null);
              otherCounter++;
              count.incrementAndGet();
            }
          } catch (IOException e) {
            throw new HopException("Error handling file " + file, e);
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error find files to import from PDI/Kettle into Hop project", e);
    }

    getLog().logBasic("We found " + count.get() + " kettle files. ");
  }

  private void handleHopFile(FileObject kettleFile) throws HopException {
    Document doc = getDocFromFile(kettleFile);

    // import connections first
    //
    importDbConnections(doc, kettleFile);

    // move to processNode?
    String extension = kettleFile.getName().getExtension();
    Element documentElement = doc.getDocumentElement();

    // We need to add an element to the document:
    //
    //   name_sync_with_filename
    //
    Element nameSync = doc.createElement("name_sync_with_filename");
    nameSync.appendChild(doc.createTextNode("Y"));
    documentElement.appendChild(nameSync);

    if (extension.equalsIgnoreCase("ktr")) {
      ktrCounter++;
      renameNode(doc, documentElement, "pipeline");

      // Add the name-sync node in /pipeline/info/
      //
      Node targetNode = XmlHandler.getSubNode(documentElement, "info");
      if (targetNode != null) {
        targetNode.insertBefore(nameSync, XmlHandler.getSubNode(targetNode, "description"));
      }
    } else if (extension.equalsIgnoreCase("kjb")) {
      kjbCounter++;
      renameNode(doc, documentElement, "workflow");

      // Add the name-sync node in /workflow/
      //
      documentElement.insertBefore(nameSync, XmlHandler.getSubNode(documentElement, "description"));
    }
    processNode(doc, documentElement);

    DOMSource domSource = new DOMSource(doc);

    // Only copy if the file doesn't exist or if we're overwriting...
    //
    getMigratedFilesMap().put(kettleFile.getName().getURI(), domSource);
  }

  /**
   * Grab the list of files to be migrated and copy them over...
   *
   * @throws HopException
   */
  @Override
  public void importFiles() throws HopException {

    try {
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");

      Iterator<String> filesIterator = getMigratedFilesMap().keySet().iterator();
      while (filesIterator.hasNext() && (monitor == null || !monitor.isCanceled())) {

        String filename = filesIterator.next();
        DOMSource domSource = getMigratedFilesMap().get(filename);

        FileObject sourceFile = HopVfs.getFileObject(filename);
        if (sourceFile.isFolder()) {
          continue;
        }

        String targetFilename =
            filename.replaceAll(inputFolder.getName().getURI(), outputFolderName);

        if (domSource != null) {
          // We need to rename the target file extensions for these pipelines and workflows...
          targetFilename =
              targetFilename.replaceAll("\\.ktr$", ".hpl").replaceAll("\\.kjb$", ".hwf");
        }

        if (monitor != null) {
          monitor.subTask("Saving file " + targetFilename);
        }

        FileObject targetFile = HopVfs.getFileObject(targetFilename);
        if (isSkippingExistingTargetFiles() && targetFile.exists()) {
          continue;
        }

        // Make sure the parent folder(s) exist...
        //
        if (!targetFile.getParent().exists()) {
          targetFile.getParent().createFolder();
        }

        if (domSource == null) {
          // copy any non-Hop files as is
          //
          try {
            NameFileFilter filter =
                new NameFileFilter(Collections.singletonList(sourceFile.getName().getBaseName()));
            targetFile.getParent().copyFrom(sourceFile.getParent(), new FileFilterSelector(filter));
          } catch (IOException e) {
            throw new HopException("Error copying file '" + filename, e);
          }
        } else {
          // Convert Kettle XML metadata to Hop (write the .hpl/.hwf)
          //
          ByteArrayOutputStream os = new ByteArrayOutputStream();

          try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            StreamResult streamResult = new StreamResult(outputStream);
            try {
              transformer.transform(domSource, streamResult);
            } catch (TransformerException e) {
              throw new HopException("Error importing file " + filename, e);
            } finally {
              outputStream.flush();
              outputStream.close();

              // Now pretty print the XML...
              //
              String xml =
                  XmlFormatter.format(
                      new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
              try (OutputStream fileStream = HopVfs.getOutputStream(targetFilename, false)) {
                fileStream.write(xml.getBytes(StandardCharsets.UTF_8));
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error importing Kettle files into Hop", e);
    }
  }

  @Override
  public void importConnections() throws HopException {
    collectConnectionsFromSharedXml();
    collectConnectionsFromJdbcProperties();
    importCollectedConnections();
    saveConnectionsReport();
  }

  private void saveConnectionsReport() throws HopException {
    // only create connections csv if we have connections
    if (connectionsList.size() > 0) {
      this.connectionsReportFileName = getOutputFolderName() + "/connections.csv";
      try (OutputStream outputStream =
          HopVfs.getOutputStream(this.connectionsReportFileName, false)) {
        for (Map.Entry<String, String> entry : connectionFileMap.entrySet()) {
          outputStream.write(entry.getKey().getBytes(StandardCharsets.UTF_8));
          outputStream.write(",".getBytes(StandardCharsets.UTF_8));
          outputStream.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
          outputStream.write(Const.CR.getBytes(StandardCharsets.UTF_8));
        }
      } catch (IOException e) {
        throw new HopException("Error writing connections.csv file to project", e);
      }
    }
  }

  private void importCollectedConnections() throws HopException {
    // Simply add the collected connections to the metadata provider...
    //
    IHopMetadataSerializer<DatabaseMeta> serializer =
        metadataProvider.getSerializer(DatabaseMeta.class);
    for (DatabaseMeta databaseMeta : connectionsList) {
      serializer.save(databaseMeta);
    }
  }

  public void collectConnectionsFromSharedXml() throws HopException {
    if (StringUtils.isEmpty(sharedXmlFilename)) {
      return;
    }
    Document doc = getDocFromFile(HopVfs.getFileObject(sharedXmlFilename));
    importDbConnections(doc, HopVfs.getFileObject(sharedXmlFilename));
  }

  public void collectConnectionsFromJdbcProperties() throws HopException {
    if (StringUtils.isEmpty(jdbcPropertiesFilename)) {
      return;
    }
    try {
      Properties properties = new Properties();
      FileObject varFile = HopVfs.getFileObject(jdbcPropertiesFilename);
      InputStream inputStream = HopVfs.getInputStream(varFile);
      properties.load(inputStream);
      List<String> connNamesList = new ArrayList<>();
      for (String connKey : properties.stringPropertyNames()) {
        String connName = connKey.split("/")[0];
        if (!connNamesList.contains(connName)) {
          connNamesList.add(connName);
        }
      }

      for (String connName : connNamesList) {
        NoneDatabaseMeta database = new NoneDatabaseMeta();
        database.setDriverClass((String) properties.get(connName + "/driver"));
        database.setManualUrl((String) properties.get(connName + "/url"));
        database.setUsername((String) properties.get(connName + "/user"));
        database.setPassword((String) properties.get(connName + "/password"));
        DatabaseMeta databaseMeta = new DatabaseMeta();
        databaseMeta.setName(connName);
        database.setPluginId(databaseMeta.getPluginName());
        databaseMeta.setIDatabase(database);

        addDatabaseMeta(varFile.getName().getURI(), databaseMeta);
      }
    } catch (Exception e) {
      throw new HopException("Error importing properties database connection", e);
    }
  }

  private void importDbConnections(Document doc, FileObject kettleFile) throws HopException {

    PluginRegistry registry = PluginRegistry.getInstance();

    NodeList connectionList = doc.getElementsByTagName("connection");
    for (int i = 0; i < connectionList.getLength(); i++) {
      if (connectionList.item(i).getParentNode().equals(doc.getDocumentElement())) {
        Element connElement = (Element) connectionList.item(i);
        String databaseType = connElement.getElementsByTagName("type").item(0).getTextContent();
        IPlugin databasePlugin =
            registry.findPluginWithId(
                DatabasePluginType.class,
                connElement.getElementsByTagName("type").item(0).getTextContent());

        try {
          DatabaseMeta databaseMeta = new DatabaseMeta();
          IDatabase iDatabase = (BaseDatabaseMeta) registry.loadClass(databasePlugin);

          if (connElement.getElementsByTagName("name").getLength() > 0) {
            databaseMeta.setName(getTextContent(connElement, "name", 0));
          }
          if (connElement.getElementsByTagName("server").getLength() > 0) {
            iDatabase.setHostname(getTextContent(connElement, "server", 0));
          }
          if (connElement.getElementsByTagName("access").getLength() > 0) {
            iDatabase.setAccessType(
                DatabaseMeta.getAccessType(getTextContent(connElement, "access", 0)));
          }
          if (connElement.getElementsByTagName("database").getLength() > 0) {
            iDatabase.setDatabaseName(getTextContent(connElement, "database", 0));
          }
          if (connElement.getElementsByTagName("port").getLength() > 0) {
            iDatabase.setPort(getTextContent(connElement, "port", 0));
          }
          if (connElement.getElementsByTagName("username").getLength() > 0) {
            iDatabase.setUsername(getTextContent(connElement, "username", 0));
          }
          if (connElement.getElementsByTagName("password").getLength() > 0) {
            iDatabase.setPassword(getTextContent(connElement, "password", 0));
          }
          if (connElement.getElementsByTagName("servername").getLength() > 0) {
            iDatabase.setServername(getTextContent(connElement, "servername", 0));
          }
          if (connElement.getElementsByTagName("tablespace").getLength() > 0) {
            iDatabase.setDataTablespace(getTextContent(connElement, "tablespace", 0));
          }
          if (connElement.getElementsByTagName("data_tablespace").getLength() > 0) {
            iDatabase.setDataTablespace(getTextContent(connElement, "data_tablespace", 0));
          }
          if (connElement.getElementsByTagName("index_tablespace").getLength() > 0) {
            iDatabase.setIndexTablespace(getTextContent(connElement, "index_tablespace", 0));
          }
          Map<String, String> attributesMap = new HashMap<>();
          NodeList connNodeList = connElement.getElementsByTagName("attributes");
          for (int j = 0; j < connNodeList.getLength(); j++) {
            if (connNodeList.item(j).getNodeName().equals("attributes")) {
              Node attributesNode = connNodeList.item(j);
              for (int k = 0; k < attributesNode.getChildNodes().getLength(); k++) {
                Node attributeNode = attributesNode.getChildNodes().item(k);
                for (int l = 0; l < attributeNode.getChildNodes().getLength(); l++) {
                  String code = "";
                  String attribute = "";
                  if (attributeNode.getChildNodes().item(l).getNodeName().equals("code")) {
                    code = attributeNode.getChildNodes().item(l).getTextContent();
                  }
                  if (attributeNode.getChildNodes().item(l).getNodeName().equals("attribute")) {
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
          databaseMeta.setDatabaseType(
              connElement.getElementsByTagName("type").item(0).getTextContent());

          addDatabaseMeta(kettleFile.getName().getURI(), databaseMeta);
        } catch (Exception e) {
          throw new HopException(
              "Error importing database type '"
                  + databaseType
                  + "' from file '"
                  + kettleFile.getName().getURI()
                  + "'",
              e);
        }
      }
    }
  }

  @Override
  public void importVariables() throws HopException {
    if (StringUtils.isEmpty(kettlePropertiesFilename)
        || StringUtils.isEmpty(targetConfigFilename)) {
      return;
    }

    collectVariablesFromKettleProperties();

    // Have the projects plugin handle the collected variables: add to project config
    //
    this.variablesTargetConfigFile = outputFolderName + "/" + targetConfigFilename;
    Object[] payload = {this.variablesTargetConfigFile, collectedVariables};
    ExtensionPointHandler.callExtensionPoint(log, variables, "HopImportVariables", payload);
  }

  private void renameNode(Document doc, Element element, String newElementName) {
    doc.renameNode(element, null, newElementName);
  }

  private void processNode(Document doc, Node node) {
    Node nodeToProcess = node;
    NodeList nodeList = nodeToProcess.getChildNodes();

    // do a first pass to remove repository definitions
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node repositoryNode = nodeList.item(i);
      if (repositoryNode.getNodeType() == Node.ELEMENT_NODE) {
        if (KettleConst.repositoryTypes.contains(repositoryNode.getTextContent())) {

          for (int j = 0; j < node.getChildNodes().getLength(); j++) {
            Node childNode = node.getChildNodes().item(j);
            if (childNode.getNodeName().equals("jobname")
                || childNode.getNodeName().equals("transname")) {
              if (!StringUtil.isEmpty(childNode.getTextContent())) {
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
        if (KettleConst.kettleElementsToRemove.containsKey(currentNode.getNodeName())) {
          if (!StringUtils.isEmpty(
              KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()))) {
            // see if we have multiple parent nodes to check for:
            if (KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()).contains(",")) {
              Node parentNode = currentNode.getParentNode();
              String[] parentNodeNames =
                  KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()).split(",");
              for (String parentNodeName : parentNodeNames) {
                if (parentNode.getNodeName().equals(parentNodeName)) {
                  parentNode.removeChild(currentNode);
                }
              }
            } else {
              if (currentNode
                  .getParentNode()
                  .getNodeName()
                  .equals(KettleConst.kettleElementsToRemove.get(currentNode.getNodeName()))) {
                currentNode.getParentNode().removeChild(currentNode);
              }
            }
          } else {
            currentNode.getParentNode().removeChild(currentNode);
          }
        }

        // rename Kettle elements to Hop elements
        if (KettleConst.kettleElementReplacements.containsKey(currentNode.getNodeName())) {
          renameNode(
              doc,
              (Element) currentNode,
              KettleConst.kettleElementReplacements.get(currentNode.getNodeName()));
        }

        // replace element contents with Hop equivalent
        if (KettleConst.kettleReplaceContent.containsKey(currentNode.getTextContent())) {
          currentNode.setTextContent(
              KettleConst.kettleReplaceContent.get(currentNode.getTextContent()));
        }

        processNode(doc, currentNode);
      }

      // partial node content replacement
      if (currentNode.getNodeType() == Node.TEXT_NODE
          && !StringUtils.isEmpty(currentNode.getTextContent())) {
        for (Map.Entry<String, String> entry : KettleConst.kettleReplaceInContent.entrySet()) {
          if (currentNode.getTextContent().contains(entry.getKey())) {
            currentNode.setTextContent(
                currentNode.getTextContent().replace(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

  private Node processRepositoryNode(Node repositoryNode) {

    String filename = "";
    String directory = "${PROJECT_HOME}";
    String type = "";
    Node filenameNode = null;

    for (int i = 0; i < repositoryNode.getChildNodes().getLength(); i++) {
      Node childNode = repositoryNode.getChildNodes().item(i);
      if (childNode.getNodeName().equals("directory")) {
        if (childNode.getTextContent().startsWith(System.getProperty("file.separator"))) {
          directory += childNode.getTextContent();
        } else {
          directory += System.getProperty("file.separator") + childNode.getTextContent();
        }
        repositoryNode.removeChild(childNode);
      }
      if (childNode.getNodeName().equals("type")) {
        if (KettleConst.jobTypes.contains(childNode.getTextContent())) {
          type = ".hwf";
        }
        if (KettleConst.transTypes.contains(childNode.getTextContent())) {
          type = ".hpl";
        }
      }
      if (childNode.getNodeName().equals("filename")) {
        filename = childNode.getTextContent().replaceAll(".ktr", "").replaceAll(".kjb", "");
        childNode.setTextContent(filename + type);
        filenameNode = childNode;
      }

      // hard coded local run configuration for now
      if (childNode.getNodeName().equals("run_configuration")) {
        childNode.setTextContent("local");
      }
      if (childNode.getNodeName().equals("jobname")
          || childNode.getNodeName().equals("transname")) {
        filename = childNode.getTextContent();
        repositoryNode.removeChild(childNode);
      }
    }

    filenameNode.setTextContent(directory + "/" + filename + type);

    return repositoryNode;
  }

  private Document getDocFromFile(FileObject kettleFile) throws HopException {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      dbFactory.setIgnoringElementContentWhitespace(true);
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(HopVfs.getInputStream(kettleFile));
      return doc;
    } catch (Exception e) {
      throw new HopException("Error importing file '" + kettleFile + "'", e);
    }
  }

  private String getTextContent(Element element, String tagName, Integer itemIndex) {
    return element.getElementsByTagName(tagName).item(itemIndex).getTextContent();
  }

  /**
   * Gets kjbCounter
   *
   * @return value of kjbCounter
   */
  public int getKjbCounter() {
    return kjbCounter;
  }

  /** @param kjbCounter The kjbCounter to set */
  public void setKjbCounter(int kjbCounter) {
    this.kjbCounter = kjbCounter;
  }

  /**
   * Gets ktrCounter
   *
   * @return value of ktrCounter
   */
  public int getKtrCounter() {
    return ktrCounter;
  }

  /** @param ktrCounter The ktrCounter to set */
  public void setKtrCounter(int ktrCounter) {
    this.ktrCounter = ktrCounter;
  }

  /**
   * Gets otherCounter
   *
   * @return value of otherCounter
   */
  public int getOtherCounter() {
    return otherCounter;
  }

  /** @param otherCounter The otherCounter to set */
  public void setOtherCounter(int otherCounter) {
    this.otherCounter = otherCounter;
  }

  /**
   * Gets variablesTargetConfigFile
   *
   * @return value of variablesTargetConfigFile
   */
  public String getVariablesTargetConfigFile() {
    return variablesTargetConfigFile;
  }

  /** @param variablesTargetConfigFile The variablesTargetConfigFile to set */
  public void setVariablesTargetConfigFile(String variablesTargetConfigFile) {
    this.variablesTargetConfigFile = variablesTargetConfigFile;
  }

  /**
   * Gets connectionsReportFileName
   *
   * @return value of connectionsReportFileName
   */
  public String getConnectionsReportFileName() {
    return connectionsReportFileName;
  }

  /** @param connectionsReportFileName The connectionsReportFileName to set */
  public void setConnectionsReportFileName(String connectionsReportFileName) {
    this.connectionsReportFileName = connectionsReportFileName;
  }
}
