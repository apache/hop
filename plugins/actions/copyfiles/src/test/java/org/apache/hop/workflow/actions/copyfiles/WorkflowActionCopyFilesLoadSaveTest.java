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
package org.apache.hop.workflow.actions.copyfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

class WorkflowActionCopyFilesLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionCopyFiles> {

  private static class CopyFilesItemValidator implements IFieldLoadSaveValidator<CopyFilesItem> {
    private final StringLoadSaveValidator strings = new StringLoadSaveValidator();

    @Override
    public CopyFilesItem getTestObject() {
      return new CopyFilesItem(
          strings.getTestObject(), strings.getTestObject(), strings.getTestObject());
    }

    @Override
    public boolean validateTestObject(CopyFilesItem original, Object actual) throws HopException {
      return actual instanceof CopyFilesItem c && original.equals(c);
    }
  }

  @Override
  protected Class<ActionCopyFiles> getActionClass() {
    return ActionCopyFiles.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(
        "copyEmptyFolders",
        "argFromPrevious",
        "overwriteFiles",
        "includeSubFolders",
        "removeSourceFiles",
        "addResultFilenames",
        "destinationIsAFile",
        "createDestinationFolder",
        "fileRows");
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
        "copyEmptyFolders", "isCopyEmptyFolders",
        "argFromPrevious", "isArgFromPrevious",
        "overwriteFiles", "isOverwriteFiles",
        "includeSubFolders", "isIncludeSubFolders",
        "removeSourceFiles", "isRemoveSourceFiles",
        "addResultFilenames", "isAddResultFilenames",
        "destinationIsAFile", "isDestinationIsAFile",
        "createDestinationFolder", "isCreateDestinationFolder",
        "fileRows", "getFileRows");
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
        "copyEmptyFolders", "setCopyEmptyFolders",
        "argFromPrevious", "setArgFromPrevious",
        "overwriteFiles", "setOverwriteFiles",
        "includeSubFolders", "setIncludeSubFolders",
        "removeSourceFiles", "setRemoveSourceFiles",
        "addResultFilenames", "setAddResultFilenames",
        "destinationIsAFile", "setDestinationIsAFile",
        "createDestinationFolder", "setCreateDestinationFolder",
        "fileRows", "setFileRows");
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    int rowCount = new Random().nextInt(5) + 1;
    Map<String, IFieldLoadSaveValidator<?>> attrMap = new HashMap<>();
    attrMap.put("fileRows", new ListLoadSaveValidator<>(new CopyFilesItemValidator(), rowCount));
    return attrMap;
  }

  private static ActionCopyFiles loadCopyFilesFromXmlResource(String resource)
      throws HopXmlException {
    InputStream in = WorkflowActionCopyFilesLoadSaveTest.class.getResourceAsStream(resource);
    assertNotNull(in, resource);
    Document document = XmlHandler.loadXmlFile(in);
    Node node = XmlHandler.getSubNode(document, ActionMeta.XML_TAG);
    ActionCopyFiles action = new ActionCopyFiles();
    action.setPluginId("COPY_FILES");
    action.loadXml(node, new MemoryMetadataProvider(), new Variables());
    return action;
  }

  @Test
  void testActionXmlSerialization() throws Exception {
    ActionCopyFiles action = loadCopyFilesFromXmlResource("/copy-files-action.xml");

    assertFalse(action.isCopyEmptyFolders());
    assertTrue(action.isArgFromPrevious());
    assertTrue(action.isOverwriteFiles());
    assertTrue(action.isIncludeSubFolders());
    assertTrue(action.isRemoveSourceFiles());
    assertTrue(action.isAddResultFilenames());
    assertTrue(action.isDestinationIsAFile());
    assertTrue(action.isCreateDestinationFolder());

    assertEquals(2, action.getFileRows().size());
    assertEquals("${SOURCE_DIR}/file1.txt", action.getFileRows().get(0).getSourceFileFolder());
    assertEquals("/data/in", action.getFileRows().get(1).getSourceFileFolder());
    assertEquals("${DEST_DIR}/out1.txt", action.getFileRows().get(0).getDestinationFileFolder());
    assertEquals("/data/out", action.getFileRows().get(1).getDestinationFileFolder());
    assertEquals("^file1\\.txt$", action.getFileRows().get(0).getWildcard());
    assertTrue(Utils.isEmpty(action.getFileRows().get(1).getWildcard()));
  }

  @Test
  void testCloneFromActionXml() throws Exception {
    ActionCopyFiles action = loadCopyFilesFromXmlResource("/copy-files-action.xml");

    ActionCopyFiles clone = (ActionCopyFiles) action.clone();

    assertEquals(clone.getFileRows(), action.getFileRows());
    assertEquals(clone.isCopyEmptyFolders(), action.isCopyEmptyFolders());
    assertEquals(clone.isArgFromPrevious(), action.isArgFromPrevious());
    assertEquals(clone.isDestinationIsAFile(), action.isDestinationIsAFile());
  }

  @Test
  void testActionXmlRoundTripThroughGetXml() throws Exception {
    ActionCopyFiles action = loadCopyFilesFromXmlResource("/copy-files-action.xml");
    String wrapped = ActionSerializationTestUtil.getXml(action);
    Document doc = XmlHandler.loadXmlString(wrapped);
    Node node = XmlHandler.getSubNode(doc, ActionMeta.XML_TAG);
    ActionCopyFiles again = new ActionCopyFiles();
    again.setPluginId("COPY_FILES");
    again.loadXml(node, new MemoryMetadataProvider(), new Variables());
    assertEquals(action.getXml(), again.getXml());
  }

  @Test
  void loadsLegacyFieldXmlWithWizardPrefixesAndConfigurationTags() throws Exception {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><action><name>t</name><description/><type>COPY_FILES</type><attributes/>"
            + "<copy_empty_folders>Y</copy_empty_folders><arg_from_previous>N</arg_from_previous>"
            + "<overwrite_files>N</overwrite_files><include_subfolders>N</include_subfolders>"
            + "<remove_source_files>N</remove_source_files><add_result_filesname>N</add_result_filesname>"
            + "<destination_is_a_file>N</destination_is_a_file><create_destination_folder>Y</create_destination_folder>"
            + "<fields><field>"
            + "<source_filefolder>EMPTY_SOURCE_URL-0-${A}</source_filefolder>"
            + "<source_configuration_name>STATIC-SOURCE-FILE-0</source_configuration_name>"
            + "<destination_filefolder>EMPTY_DEST_URL-0-${A}/b</destination_filefolder>"
            + "<destination_configuration_name>STATIC-DEST-FILE-0</destination_configuration_name>"
            + "<wildcard>x</wildcard>"
            + "</field></fields></action>";
    Document doc = XmlHandler.loadXmlString(xml);
    Node node = XmlHandler.getSubNode(doc, ActionMeta.XML_TAG);
    ActionCopyFiles a = new ActionCopyFiles();
    a.setPluginId("COPY_FILES");
    a.loadXml(node, new MemoryMetadataProvider(), new Variables());
    assertEquals("${A}", a.getFileRows().get(0).getSourceFileFolder());
    assertEquals("${A}/b", a.getFileRows().get(0).getDestinationFileFolder());
    assertEquals("x", a.getFileRows().get(0).getWildcard());
  }

  @Test
  void legacyRowPrefixesAreStrippedOnLoadAndOmittedOnReserialize() throws Exception {
    ActionCopyFiles a = new ActionCopyFiles();
    a.setPluginId("COPY_FILES");
    a.setFileRows(
        List.of(
            new CopyFilesItem(
                ActionCopyFiles.SOURCE_URL + "0-my/src",
                ActionCopyFiles.DEST_URL + "0-my/dest",
                ".*")));
    String wrapped = ActionSerializationTestUtil.getXml(a);
    Document doc = XmlHandler.loadXmlString(wrapped);
    Node node = XmlHandler.getSubNode(doc, ActionMeta.XML_TAG);
    ActionCopyFiles loaded = new ActionCopyFiles();
    loaded.setPluginId("COPY_FILES");
    loaded.loadXml(node, new MemoryMetadataProvider(), new Variables());
    assertEquals("my/src", loaded.getFileRows().get(0).getSourceFileFolder());
    assertEquals("my/dest", loaded.getFileRows().get(0).getDestinationFileFolder());
    String out = loaded.getXml();
    assertFalse(out.contains(ActionCopyFiles.SOURCE_URL));
    assertFalse(out.contains(ActionCopyFiles.DEST_URL));
    assertFalse(out.contains("source_configuration_name"));
    assertFalse(out.contains("destination_configuration_name"));
  }
}
