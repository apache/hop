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
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileFilterSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.filter.NameFileFilter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlFormatter;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.imp.HopImportBase;
import org.apache.hop.imp.IHopImport;
import org.apache.hop.imp.ImportPlugin;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@ImportPlugin(
    id = "kettle",
    name = "Kettle Import",
    description = "Imports Kettle/PDI files, metadata and variables",
    documentationUrl = "/plugins/import/kettle-import.html")
public class KettleImport extends HopImportBase implements IHopImport {
  private static final Class<?> PKG = KettleImport.class;
  public static final String CONST_SERVERNAME = "servername";
  public static final String CONST_TABLESPACE = "tablespace";
  public static final String CONST_DATA_TABLESPACE = "data_tablespace";
  public static final String CONST_INDEX_TABLESPACE = "index_tablespace";

  private int kjbCounter;
  private int ktrCounter;
  private int otherCounter;
  private String variablesTargetConfigFile;
  private String connectionsReportFileName;

  private enum EntryType {
    TRANS,
    JOB,
    START,
    DUMMY,
    FORMULA,
    GOOGLE_SHEETS_INPUT,
    METAINJECT,
    SIMPLE_MAPPING,
    OTHER
  };

  public KettleImport() {
    super();
  }

  @Override
  public void findFilesToImport() throws HopException {
    AtomicInteger count = new AtomicInteger();
    try {
      // Reset counters
      this.kjbCounter = 0;
      this.ktrCounter = 0;
      this.otherCounter = 0;

      // Find all files...
      //
      List<FileObject> allFiles = new ArrayList<>();
      allFiles.addAll(HopVfs.findFiles(getInputFolder(), null, !skippingFolders));

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

    processNode(doc, documentElement, EntryType.OTHER, 0);

    // Align x/y locations with a grid size...
    //
    int gridSize = Const.toInt(new Props().getProperty("CanvasGridSize"), 16);
    if (gridSize > 1) {
      alignLocations(documentElement, gridSize);
    }

    // Keep the document for later saving...
    //
    DOMSource domSource = new DOMSource(doc);
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
      TransformerFactory transformerFactory = XmlHandler.createSecureTransformerFactory();
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
    if (!connectionsList.isEmpty()) {
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
          databaseMeta.setIDatabase(iDatabase);
          databaseMeta.setDatabaseType(databaseType);

          if (connElement.getElementsByTagName("name").getLength() > 0) {
            databaseMeta.setName(getTextContent(connElement, "name", 0));
          }
          if (connElement.getElementsByTagName("server").getLength() > 0) {
            databaseMeta.getIDatabase().setHostname(getTextContent(connElement, "server", 0));
          }
          if (connElement.getElementsByTagName("access").getLength() > 0) {
            databaseMeta
                .getIDatabase()
                .setAccessType(
                    DatabaseMeta.getAccessType(getTextContent(connElement, "access", 0)));
          }
          if (connElement.getElementsByTagName("database").getLength() > 0) {
            databaseMeta.getIDatabase().setDatabaseName(getTextContent(connElement, "database", 0));
          }
          if (connElement.getElementsByTagName("port").getLength() > 0) {
            databaseMeta.getIDatabase().setPort(getTextContent(connElement, "port", 0));
          }
          if (connElement.getElementsByTagName("username").getLength() > 0) {
            databaseMeta.getIDatabase().setUsername(getTextContent(connElement, "username", 0));
          }
          if (connElement.getElementsByTagName("password").getLength() > 0) {
            databaseMeta.getIDatabase().setPassword(getTextContent(connElement, "password", 0));
          }
          if (connElement.getElementsByTagName(CONST_SERVERNAME).getLength() > 0
              && !Utils.isEmpty(getTextContent(connElement, CONST_SERVERNAME, 0))) {
            databaseMeta
                .getIDatabase()
                .setServername(getTextContent(connElement, CONST_SERVERNAME, 0));
          }
          if (connElement.getElementsByTagName(CONST_TABLESPACE).getLength() > 0
              && !Utils.isEmpty(getTextContent(connElement, CONST_TABLESPACE, 0))) {
            databaseMeta
                .getIDatabase()
                .setDataTablespace(getTextContent(connElement, CONST_TABLESPACE, 0));
          }
          if (connElement.getElementsByTagName(CONST_DATA_TABLESPACE).getLength() > 0
              && !Utils.isEmpty(getTextContent(connElement, CONST_DATA_TABLESPACE, 0))) {
            databaseMeta
                .getIDatabase()
                .setDataTablespace(getTextContent(connElement, CONST_DATA_TABLESPACE, 0));
          }
          if (connElement.getElementsByTagName(CONST_INDEX_TABLESPACE).getLength() > 0
              && !Utils.isEmpty(getTextContent(connElement, CONST_INDEX_TABLESPACE, 0))) {
            databaseMeta
                .getIDatabase()
                .setIndexTablespace(getTextContent(connElement, CONST_INDEX_TABLESPACE, 0));
          }
          Map<String, String> attributesMap = new HashMap<>();
          NodeList connNodeList = connElement.getElementsByTagName("attributes");
          for (int j = 0; j < connNodeList.getLength(); j++) {
            if (connNodeList.item(j).getNodeName().equals("attributes")) {
              Node attributesNode = connNodeList.item(j);
              for (int k = 0; k < attributesNode.getChildNodes().getLength(); k++) {
                Node attributeNode = attributesNode.getChildNodes().item(k);
                String code = "";
                String attribute = "";
                for (int l = 0; l < attributeNode.getChildNodes().getLength(); l++) {
                  if (attributeNode.getChildNodes().item(l).getNodeName().equals("code")) {
                    code = attributeNode.getChildNodes().item(l).getTextContent();
                  }
                  if (attributeNode.getChildNodes().item(l).getNodeName().equals("attribute")) {
                    attribute = attributeNode.getChildNodes().item(l).getTextContent();
                  }
                  if (!Utils.isEmpty(code) && !Utils.isEmpty(attribute)) {
                    attributesMap.put(code, attribute);
                  }
                }
              }
            }
          }

          databaseMeta.getIDatabase().setAttributes(attributesMap);
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

  private void processNode(Document doc, Node node, EntryType entryType, int depth) {
    Node nodeToProcess = node;
    NodeList nodeList = nodeToProcess.getChildNodes();

    // Set depth level
    depth += 1;

    // do a first pass to remove repository definitions
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node repositoryNode = nodeList.item(i);
      if (repositoryNode.getNodeType() == Node.ELEMENT_NODE
          && KettleConst.repositoryTypes.contains(repositoryNode.getTextContent())) {
        for (int j = 0; j < node.getChildNodes().getLength(); j++) {
          Node childNode = node.getChildNodes().item(j);
          if ((childNode.getNodeName().equals("jobname")
                  || childNode.getNodeName().equals("transname")
                  || childNode.getNodeName().equals("trans_name"))
              && !StringUtil.isEmpty(childNode.getTextContent())) {
            nodeToProcess = processRepositoryNode(node);
          }
        }
        nodeList = nodeToProcess.getChildNodes();
      }
    }

    Node firstFormulaNode = null;

    for (int i = 0; i < nodeList.getLength(); i++) {
      Node currentNode = nodeList.item(i);

      if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
        // Identify if an entry is of type START or DUMMY type because they must be managed properly
        if (currentNode.getNodeName().equals("entry")) {
          entryType = EntryType.OTHER;
          Node entryTypeNode = null;
          boolean isEntryTypeSpecial = false;
          NodeList currentNodeChildNodes = currentNode.getChildNodes();
          for (int i1 = 0; i1 < currentNodeChildNodes.getLength(); i1++) {
            Node childNode = currentNodeChildNodes.item(i1);
            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
              if (childNode.getNodeName().equals("type")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("SPECIAL")) {
                isEntryTypeSpecial = true;
                entryTypeNode = childNode;
              } else if (childNode.getNodeName().equals("type")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("TRANS")) {
                entryType = EntryType.TRANS;
              } else if (childNode.getNodeName().equals("type")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("JOB")) {
                entryType = EntryType.JOB;
              } else if (isEntryTypeSpecial
                  && childNode.getNodeName().equals("start")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("Y")) {
                entryType = EntryType.START;
              } else if (isEntryTypeSpecial
                  && childNode.getNodeName().equals("dummy")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("Y")) {
                entryType = EntryType.DUMMY;
                // Immediately change entry type to DUMMY to not bother about it later on
                entryTypeNode.getFirstChild().setTextContent("DUMMY");
              }
            }
          }
        }

        if (currentNode.getNodeName().equals("step")) {
          entryType = EntryType.OTHER;
          NodeList currentNodeChildNodes = currentNode.getChildNodes();
          for (int i1 = 0; i1 < currentNodeChildNodes.getLength(); i1++) {
            Node childNode = currentNodeChildNodes.item(i1);
            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
              if (childNode.getNodeName().equals("type")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("Formula")) {
                entryType = EntryType.FORMULA;
              }
              if (childNode.getNodeName().equals("type")
                  && childNode
                      .getChildNodes()
                      .item(0)
                      .getNodeValue()
                      .equals("PentahoGoogleSheetsPluginInputMeta")) {
                entryType = EntryType.GOOGLE_SHEETS_INPUT;
              }
              if (childNode.getNodeName().equals("type")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("Mapping")) {
                entryType = EntryType.SIMPLE_MAPPING;
              }
              if (childNode.getNodeName().equals("type")
                  && childNode.getChildNodes().item(0).getNodeValue().equals("MetaInject")) {
                entryType = EntryType.METAINJECT;
              }
            }
          }
        }

        // remove superfluous elements
        if (entryType == EntryType.OTHER) {
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
        } else if (entryType == EntryType.START) {
          if (KettleConst.kettleStartEntryElementsToRemove.containsKey(currentNode.getNodeName())) {
            currentNode.getParentNode().removeChild(currentNode);
          }
        } else if (entryType == EntryType.DUMMY
            && KettleConst.kettleDummyEntryElementsToRemove.containsKey(
                currentNode.getNodeName())) {
          currentNode.getParentNode().removeChild(currentNode);
        }

        if (entryType == EntryType.FORMULA && currentNode.getNodeName().equals("formula")) {
          if (firstFormulaNode == null) {
            Element formulasElement = doc.createElement("formulas");
            formulasElement.appendChild(currentNode);
            firstFormulaNode = formulasElement;
          } else {
            // Append new one to collection of new formula nodes
            firstFormulaNode.appendChild(currentNode);
          }
        } else if (entryType == EntryType.FORMULA
            && !currentNode.getNodeName().equals("formula")
            && firstFormulaNode != null
            && depth == 2) {
          // Always in same steps' nodeset, same depth level and we finished formula definition
          // nodes
          // Add new collections to formlua nodeset
          node.appendChild(firstFormulaNode);
          // reset the entry type to OTHER because we left the formula nodeset
          entryType = EntryType.OTHER;
          firstFormulaNode = null;
        } else if (entryType == EntryType.FORMULA
            && currentNode.getNodeName().equals("formula_string")) {
          String formulaValue = currentNode.getFirstChild().getNodeValue();
          currentNode.getFirstChild().setNodeValue(formulaValue.replaceAll(";", ","));
        } else if (entryType == EntryType.FORMULA
            && currentNode.getNodeName().equals("value_type")) {
          String formulaType = currentNode.getFirstChild().getNodeValue();
          currentNode
              .getFirstChild()
              .setNodeValue(Integer.toString(ValueMetaFactory.getIdForValueMeta(formulaType)));
        }

        if ((entryType == EntryType.JOB || entryType == EntryType.TRANS)
            && currentNode.getNodeName().equals("run_configuration")) {
          if (entryType == EntryType.JOB)
            currentNode.setTextContent(defaultWorkflowRunConfiguration);
          else if (entryType == EntryType.TRANS)
            currentNode.setTextContent(defaultPipelineRunConfiguration);
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

        processNode(doc, currentNode, entryType, depth);
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

      if ((entryType == EntryType.SIMPLE_MAPPING || entryType == EntryType.METAINJECT)
          && currentNode.getNodeName().equals("transform")) {

        Node filenameNode = null;
        String transName = "";
        String directoryPath = "";
        // get trans name, file name, path, set correct filename when needed.
        for (int j = 0; j < currentNode.getChildNodes().getLength(); j++) {
          if (currentNode.getChildNodes().item(j).getNodeName().equals("directory_path")) {
            directoryPath = currentNode.getChildNodes().item(j).getTextContent();
            currentNode.removeChild(currentNode.getChildNodes().item(j));
          }
          if (currentNode.getChildNodes().item(j).getNodeName().equals("trans_name")) {
            transName = currentNode.getChildNodes().item(j).getTextContent();
            currentNode.removeChild(currentNode.getChildNodes().item(j));
          }
          if (currentNode.getChildNodes().item(j).getNodeName().equals("filename")) {
            filenameNode = currentNode.getChildNodes().item(j);
          }
        }

        // if we have a trans name and directory path, use it to update the mapping or injectable
        // pipeline
        // filename.
        if (!StringUtils.isEmpty(transName) && !StringUtils.isEmpty(directoryPath)) {
          filenameNode.setTextContent("${PROJECT_HOME}" + directoryPath + '/' + transName + ".hpl");
        }

        // add the default pipeline run configuration.
        Element runConfigElement = doc.createElement("runConfiguration");
        runConfigElement.appendChild(doc.createTextNode(defaultPipelineRunConfiguration));
        currentNode.appendChild(runConfigElement);
      }

      if (entryType == EntryType.GOOGLE_SHEETS_INPUT
          && currentNode.getNodeName().equals("jsonCredentialPath")) {
        String jsonCredentialKeyPath = currentNode.getTextContent();
        currentNode.setTextContent(jsonCredentialKeyPath.replace('\\', '/'));
      }
      if (entryType == EntryType.GOOGLE_SHEETS_INPUT && currentNode.getNodeName().equals("field")) {

        // get the second (1) child node to replace data types
        for (int j = 0; j < currentNode.getChildNodes().getLength(); j++) {
          Node childNode = currentNode.getChildNodes().item(j);
          if (childNode.getNodeName().equals("type")) {
            String typeNodeValue = childNode.getTextContent();
            switch (typeNodeValue) {
              case "Avro Record":
                childNode.setTextContent("20");
                break;
              case "BigNumber":
                childNode.setTextContent("6");
                break;
              case "Binary":
                childNode.setTextContent("8");
                break;
              case "Boolean":
                childNode.setTextContent("4");
                break;
              case "Date":
                childNode.setTextContent("3");
                break;
              case "Integer":
                childNode.setTextContent("5");
                break;
              case "Internet Address":
                childNode.setTextContent("10");
                break;
              case "Number":
                childNode.setTextContent("1");
                break;
              case "Serializable":
                childNode.setTextContent("7");
                break;
              case "String":
                childNode.setTextContent("2");
                break;
              case "Timestamp":
                childNode.setTextContent("9");
                break;
                // default to String
              default:
                childNode.setTextContent("2");
                break;
            }
          }
          if (childNode.getNodeName().equals("trim_type")) {
            String trimTypeNode = childNode.getTextContent();
            switch (trimTypeNode) {
              case "none":
                childNode.setTextContent("0");
                break;
              case "left":
                childNode.setTextContent("1");
                break;
              case "right":
                childNode.setTextContent("2");
                break;
              case "both":
                childNode.setTextContent("3");
                break;
                // don't trim if not known
              default:
                childNode.setTextContent("0");
                break;
            }
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
      DocumentBuilderFactory dbFactory = XmlParserFactoryProducer.createSecureDocBuilderFactory();
      dbFactory.setIgnoringElementContentWhitespace(true);
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      return dBuilder.parse(HopVfs.getInputStream(kettleFile));
    } catch (Exception e) {
      throw new HopException("Error importing file '" + kettleFile + "'", e);
    }
  }

  private String getTextContent(Element element, String tagName, Integer itemIndex) {
    return element.getElementsByTagName(tagName).item(itemIndex).getTextContent();
  }

  private void alignLocations(Node parentNode, int gridSize) {
    NodeList childNodes = parentNode.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node childNode = childNodes.item(i);
      String nodeName = childNode.getNodeName();
      if (nodeName.equals("xloc") || nodeName.equals("yloc")) {
        int value = Const.toInt(childNode.getTextContent(), 0);
        childNode.setTextContent(Integer.toString(gridSize * Math.round(value / gridSize)));
      }
      // Find more children
      //
      alignLocations(childNode, gridSize);
    }
  }

  @Override
  public String getImportReport() {
    String eol = System.getProperty("line.separator");
    String messageString =
        BaseMessages.getString(PKG, "KettleImportDialog.ImportSummary.Imported.Label") + eol;
    if (getKjbCounter() > 0) {
      messageString +=
          getKjbCounter()
              + " "
              + BaseMessages.getString(PKG, "KettleImportDialog.ImportSummary.ImportedJobs.Label")
              + eol;
    }
    if (getKtrCounter() > 0) {
      messageString +=
          getKtrCounter()
              + " "
              + BaseMessages.getString(PKG, "KettleImportDialog.ImportSummary.ImportedTransf.Label")
              + eol;
    }
    if (getOtherCounter() > 0) {
      messageString +=
          getOtherCounter()
              + " "
              + BaseMessages.getString(PKG, "KettleImportDialog.ImportSummary.ImportedOther.Label")
              + eol;
    }
    if (getVariableCounter() > 0) {
      messageString +=
          getVariableCounter()
              + " variables were imported into environment config file "
              + getVariablesTargetConfigFile()
              + eol
              + "You can use this as a configuration file in an environment."
              + eol;
    }
    if (getConnectionCounter() > 0) {
      messageString +=
          getConnectionCounter()
              + " database connections where saved in metadata folder "
              + getMetadataTargetFolder()
              + eol
              + eol;
      messageString +=
          "Connections with the same name and different configurations have only been saved once."
              + eol;
      messageString +=
          "Check the following file for a list of connections that might need extra attention: "
              + getConnectionsReportFileName();
    }

    return messageString;
  }

  /**
   * Gets kjbCounter
   *
   * @return value of kjbCounter
   */
  public int getKjbCounter() {
    return kjbCounter;
  }

  /**
   * @param kjbCounter The kjbCounter to set
   */
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

  /**
   * @param ktrCounter The ktrCounter to set
   */
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

  /**
   * @param otherCounter The otherCounter to set
   */
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

  /**
   * @param variablesTargetConfigFile The variablesTargetConfigFile to set
   */
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

  /**
   * @param connectionsReportFileName The connectionsReportFileName to set
   */
  public void setConnectionsReportFileName(String connectionsReportFileName) {
    this.connectionsReportFileName = connectionsReportFileName;
  }
}
