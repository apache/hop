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

package org.apache.hop.pipeline.transforms.csvinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformLoadSaveTester;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputMeta;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

class CsvInputMetaTest implements IInitializer<ITransformMeta> {
  TransformLoadSaveTester<CsvInputMeta> transformLoadSaveTester;
  Class<CsvInputMeta> testMetaClass = CsvInputMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static class TextFileInputFieldValidator
      implements IFieldLoadSaveValidator<TextFileInputField> {
    @Override
    public TextFileInputField getTestObject() {
      return new TextFileInputField(
          UUID.randomUUID().toString(), new Random().nextInt(), new Random().nextInt());
    }

    @Override
    public boolean validateTestObject(TextFileInputField testObject, Object actual) {
      if (!(actual instanceof TextFileInputField)) {
        return false;
      }

      TextFileInputField another = (TextFileInputField) actual;
      return new EqualsBuilder()
          .append(testObject.getName(), another.getName())
          .append(testObject.getLength(), another.getLength())
          .append(testObject.getType(), another.getType())
          .append(testObject.getTrimType(), another.getTrimType())
          .isEquals();
    }
  }

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        Arrays.asList(
            "bufferSize",
            "delimiter",
            "enclosure",
            "encoding",
            "filename",
            "filenameField",
            "inputFields",
            "rowNumField",
            "addResult",
            "headerPresent",
            "includingFilename",
            "lazyConversionActive",
            "newlinePossibleInFields",
            "runningInParallel");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("inputFields", "getInputFields");
            put("hasHeader", "hasHeader");
            put("includeFilename", "includeFilename");
            put("includeRowNumber", "includeRowNumber");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
          {
            put("inputFields", "setInputFields");
            put("includeFilename", "includeFilename");
            put("includeRowNumber", "includeRowNumber");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "inputFields", new ArrayLoadSaveValidator<>(new TextFileInputFieldValidator(), 5));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    transformLoadSaveTester =
        new TransformLoadSaveTester(
            testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap);
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof CsvInputMeta) {
      ((CsvInputMeta) someMeta).allocate(5);
    }
  }

  @Test
  void testSerialization() throws HopException {
    transformLoadSaveTester.testSerialization();
  }

  @Test
  void testClone() {
    final CsvInputMeta original = new CsvInputMeta();
    original.setDelimiter(";");
    original.setEnclosure("'");
    final TextFileInputField[] originalFields = new TextFileInputField[1];
    final TextFileInputField originalField = new TextFileInputField();
    originalField.setName("field");
    originalFields[0] = originalField;
    original.setInputFields(originalFields);

    final CsvInputMeta clone = (CsvInputMeta) original.clone();
    // verify that the clone and its input fields are "equal" to the originals, but not the same
    // objects
    assertNotSame(original, clone);
    assertEquals(original.getDelimiter(), clone.getDelimiter());
    assertEquals(original.getEnclosure(), clone.getEnclosure());

    assertNotSame(original.getInputFields(), clone.getInputFields());
    assertNotSame(original.getInputFields()[0], clone.getInputFields()[0]);
    assertEquals(original.getInputFields()[0].getName(), clone.getInputFields()[0].getName());
  }

  @Test
  void testSetDefault() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setDefault();

    assertEquals(",", meta.getDelimiter());
    assertEquals("\"", meta.getEnclosure());
    assertTrue(meta.isHeaderPresent());
    assertTrue(meta.isLazyConversionActive());
    assertFalse(meta.isAddResult());
    assertEquals("50000", meta.getBufferSize());
  }

  @Test
  void testGetXml() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename("test.csv");
    meta.setFilenameField("filenameField");
    meta.setRowNumField("rownum");
    meta.setIncludingFilename(true);
    meta.setDelimiter(";");
    meta.setEnclosure("'");
    meta.setHeaderPresent(false);
    meta.setBufferSize("10000");
    meta.setSchemaDefinition("testSchema");
    meta.setIgnoreFields(true);
    meta.setLazyConversionActive(false);
    meta.setAddResult(true);
    meta.setRunningInParallel(true);
    meta.setNewlinePossibleInFields(false);
    meta.setEncoding("UTF-8");

    TextFileInputField field = new TextFileInputField();
    field.setName("testField");
    field.setType(IValueMeta.TYPE_STRING);
    field.setFormat("format");
    field.setCurrencySymbol("$");
    field.setDecimalSymbol(".");
    field.setGroupSymbol(",");
    field.setLength(10);
    field.setPrecision(2);
    field.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    meta.setInputFields(new TextFileInputField[] {field});

    String xml = meta.getXml();

    assertNotNull(xml);
    assertTrue(xml.contains("test.csv"));
    assertTrue(xml.contains("filenameField"));
    assertTrue(xml.contains("rownum"));
    assertTrue(xml.contains(";"));
    assertTrue(xml.contains("<enclosure>"));
    assertTrue(xml.contains("10000"));
    assertTrue(xml.contains("testSchema"));
    assertTrue(xml.contains("UTF-8"));
    assertTrue(xml.contains("testField"));
  }

  @Test
  void testLoadXml() throws Exception {
    String xml =
        "<transform>"
            + "  <filename>test.csv</filename>"
            + "  <filename_field>fileField</filename_field>"
            + "  <rownum_field>rowField</rownum_field>"
            + "  <include_filename>Y</include_filename>"
            + "  <separator>;</separator>"
            + "  <enclosure>'</enclosure>"
            + "  <buffer_size>20000</buffer_size>"
            + "  <schemaDefinition>schema1</schemaDefinition>"
            + "  <ignoreFields>Y</ignoreFields>"
            + "  <header>N</header>"
            + "  <lazy_conversion>N</lazy_conversion>"
            + "  <add_filename_result>Y</add_filename_result>"
            + "  <parallel>Y</parallel>"
            + "  <newline_possible>N</newline_possible>"
            + "  <encoding>UTF-16</encoding>"
            + "  <fields>"
            + "    <field>"
            + "      <name>field1</name>"
            + "      <type>String</type>"
            + "      <format>format1</format>"
            + "      <currency>€</currency>"
            + "      <decimal>,</decimal>"
            + "      <group>.</group>"
            + "      <length>20</length>"
            + "      <precision>5</precision>"
            + "      <trim_type>both</trim_type>"
            + "    </field>"
            + "  </fields>"
            + "</transform>";

    Node node = XmlHandler.loadXmlString(xml).getDocumentElement();
    CsvInputMeta meta = new CsvInputMeta();
    meta.loadXml(node, new MemoryMetadataProvider());

    assertEquals("test.csv", meta.getFilename());
    assertEquals("fileField", meta.getFilenameField());
    assertEquals("rowField", meta.getRowNumField());
    assertTrue(meta.isIncludingFilename());
    assertEquals(";", meta.getDelimiter());
    assertEquals("'", meta.getEnclosure());
    assertEquals("20000", meta.getBufferSize());
    assertEquals("schema1", meta.getSchemaDefinition());
    assertTrue(meta.isIgnoreFields());
    assertFalse(meta.isHeaderPresent());
    assertFalse(meta.isLazyConversionActive());
    assertTrue(meta.isAddResult());
    assertTrue(meta.isRunningInParallel());
    assertFalse(meta.isNewlinePossibleInFields());
    assertEquals("UTF-16", meta.getEncoding());

    TextFileInputField[] fields = meta.getInputFields();
    assertEquals(1, fields.length);
    assertEquals("field1", fields[0].getName());
    assertEquals(IValueMeta.TYPE_STRING, fields[0].getType());
    assertEquals("format1", fields[0].getFormat());
    assertEquals("€", fields[0].getCurrencySymbol());
    assertEquals(",", fields[0].getDecimalSymbol());
    assertEquals(".", fields[0].getGroupSymbol());
    assertEquals(20, fields[0].getLength());
    assertEquals(5, fields[0].getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, fields[0].getTrimType());
  }

  @Test
  void testLoadXmlWithEmptyNewlinePossible_Parallel() throws Exception {
    String xml =
        "<transform>"
            + "  <filename>test.csv</filename>"
            + "  <separator>,</separator>"
            + "  <enclosure>\"</enclosure>"
            + "  <parallel>Y</parallel>"
            + "  <fields></fields>"
            + "</transform>";

    Node node = XmlHandler.loadXmlString(xml).getDocumentElement();
    CsvInputMeta meta = new CsvInputMeta();
    meta.loadXml(node, new MemoryMetadataProvider());

    assertTrue(meta.isRunningInParallel());
    assertFalse(meta.isNewlinePossibleInFields());
  }

  @Test
  void testLoadXmlWithEmptyNewlinePossible_NotParallel() throws Exception {
    String xml =
        "<transform>"
            + "  <filename>test.csv</filename>"
            + "  <separator>,</separator>"
            + "  <enclosure>\"</enclosure>"
            + "  <parallel>N</parallel>"
            + "  <fields></fields>"
            + "</transform>";

    Node node = XmlHandler.loadXmlString(xml).getDocumentElement();
    CsvInputMeta meta = new CsvInputMeta();
    meta.loadXml(node, new MemoryMetadataProvider());

    assertFalse(meta.isRunningInParallel());
    assertTrue(meta.isNewlinePossibleInFields());
  }

  @Test
  void testLoadXmlWithInvalidXml() {
    String xml = "<transform><invalidTag></transform>";

    // Should throw HopXmlException for invalid XML
    assertThrows(
        HopXmlException.class,
        () -> {
          Node node = XmlHandler.loadXmlString(xml).getDocumentElement();
          CsvInputMeta meta = new CsvInputMeta();
          meta.loadXml(node, new MemoryMetadataProvider());
        });
  }

  @Test
  void testGetFieldsWithoutIgnoreFields() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setIgnoreFields(false);
    meta.setLazyConversionActive(true);
    meta.setEncoding("UTF-8");

    TextFileInputField field = new TextFileInputField();
    field.setName("field1");
    field.setType(IValueMeta.TYPE_STRING);
    field.setFormat("format");
    field.setLength(10);
    field.setPrecision(2);
    field.setDecimalSymbol(".");
    field.setGroupSymbol(",");
    field.setCurrencySymbol("$");
    field.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    meta.setInputFields(new TextFileInputField[] {field});

    IRowMeta rowMeta = new RowMeta();
    IVariables variables = new Variables();
    meta.getFields(rowMeta, "testOrigin", null, null, variables, new MemoryMetadataProvider());

    assertEquals(1, rowMeta.size());
    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals("field1", valueMeta.getName());
    assertEquals(IValueMeta.TYPE_STRING, valueMeta.getType());
    assertEquals(IValueMeta.STORAGE_TYPE_BINARY_STRING, valueMeta.getStorageType());
    assertEquals("testOrigin", valueMeta.getOrigin());
  }

  @Test
  void testGetFieldsWithFilenameField() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setIgnoreFields(false);
    meta.setInputFields(new TextFileInputField[0]);
    meta.setFilenameField("filename");
    meta.setIncludingFilename(true);
    meta.setLazyConversionActive(true);

    IRowMeta rowMeta = new RowMeta();
    IVariables variables = new Variables();
    meta.getFields(rowMeta, "testOrigin", null, null, variables, new MemoryMetadataProvider());

    assertEquals(1, rowMeta.size());
    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals("filename", valueMeta.getName());
    assertTrue(valueMeta instanceof ValueMetaString);
    assertEquals(IValueMeta.STORAGE_TYPE_BINARY_STRING, valueMeta.getStorageType());
  }

  @Test
  void testGetFieldsWithRowNumField() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setIgnoreFields(false);
    meta.setInputFields(new TextFileInputField[0]);
    meta.setRowNumField("rownum");

    IRowMeta rowMeta = new RowMeta();
    IVariables variables = new Variables();
    meta.getFields(rowMeta, "testOrigin", null, null, variables, new MemoryMetadataProvider());

    assertEquals(1, rowMeta.size());
    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals("rownum", valueMeta.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, valueMeta.getType());
    assertEquals(10, valueMeta.getLength());
  }

  @Test
  void testGetFieldsWithoutLazyConversion() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setIgnoreFields(false);
    meta.setLazyConversionActive(false);

    TextFileInputField field = new TextFileInputField();
    field.setName("field1");
    field.setType(IValueMeta.TYPE_STRING);
    meta.setInputFields(new TextFileInputField[] {field});

    IRowMeta rowMeta = new RowMeta();
    IVariables variables = new Variables();
    meta.getFields(rowMeta, "testOrigin", null, null, variables, new MemoryMetadataProvider());

    assertEquals(1, rowMeta.size());
    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, valueMeta.getStorageType());
  }

  @Test
  void testCheck() {
    CsvInputMeta meta = new CsvInputMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IVariables variables = new Variables();
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

    // Test with no previous transform
    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        null,
        new String[0],
        new String[0],
        null,
        variables,
        metadataProvider);

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }

  @Test
  void testCheckWithPreviousTransform() {
    CsvInputMeta meta = new CsvInputMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IVariables variables = new Variables();
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

    IRowMeta prevRowMeta = new RowMeta();
    prevRowMeta.addValueMeta(new ValueMetaString("field1"));

    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prevRowMeta,
        new String[0],
        new String[0],
        null,
        variables,
        metadataProvider);

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
  }

  @Test
  void testCheckWithInput() {
    CsvInputMeta meta = new CsvInputMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = mock(TransformMeta.class);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IVariables variables = new Variables();
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        null,
        new String[] {"input1"},
        new String[0],
        null,
        variables,
        metadataProvider);

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(1).getType());
  }

  @Test
  void testGetResourceDependenciesWithFilename() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename("test.csv");
    TransformMeta transformMeta = mock(TransformMeta.class);
    IVariables variables = new Variables();

    List<ResourceReference> references = meta.getResourceDependencies(variables, transformMeta);

    assertNotNull(references);
    assertEquals(1, references.size());
    assertEquals(1, references.get(0).getEntries().size());
    assertEquals("test.csv", references.get(0).getEntries().get(0).getResource());
  }

  @Test
  void testGetResourceDependenciesWithoutFilename() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename(null);
    TransformMeta transformMeta = mock(TransformMeta.class);
    IVariables variables = new Variables();

    List<ResourceReference> references = meta.getResourceDependencies(variables, transformMeta);

    assertNotNull(references);
    assertEquals(1, references.size());
    assertEquals(0, references.get(0).getEntries().size());
  }

  @Test
  void testExportResourcesWithFilenameField() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilenameField("fieldName");
    meta.setFilename("test.csv");
    IVariables variables = new Variables();
    Map<String, ResourceDefinition> definitions = new HashMap<>();
    IResourceNaming resourceNaming = mock(IResourceNaming.class);
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

    String result = meta.exportResources(variables, definitions, resourceNaming, metadataProvider);

    assertNull(result);
  }

  @Test
  void testExportResourcesWithEmptyFilename() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename(null);
    IVariables variables = new Variables();
    Map<String, ResourceDefinition> definitions = new HashMap<>();
    IResourceNaming resourceNaming = mock(IResourceNaming.class);
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

    String result = meta.exportResources(variables, definitions, resourceNaming, metadataProvider);

    assertNull(result);
  }

  @Test
  void testSupportsErrorHandling() {
    CsvInputMeta meta = new CsvInputMeta();
    assertTrue(meta.supportsErrorHandling());
  }

  @Test
  void testGetFileType() {
    CsvInputMeta meta = new CsvInputMeta();
    assertEquals("CSV", meta.getFileType());
  }

  @Test
  void testGetSeparator() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setDelimiter(";");
    assertEquals(";", meta.getSeparator());
  }

  @Test
  void testIncludeFilename() {
    CsvInputMeta meta = new CsvInputMeta();
    assertFalse(meta.includeFilename());
  }

  @Test
  void testIncludeRowNumber() {
    CsvInputMeta meta = new CsvInputMeta();
    assertFalse(meta.includeRowNumber());
  }

  @Test
  void testIsErrorIgnored() {
    CsvInputMeta meta = new CsvInputMeta();
    assertFalse(meta.isErrorIgnored());
  }

  @Test
  void testIsErrorLineSkipped() {
    CsvInputMeta meta = new CsvInputMeta();
    assertFalse(meta.isErrorLineSkipped());
  }

  @Test
  void testGetErrorCountField() {
    CsvInputMeta meta = new CsvInputMeta();
    assertNull(meta.getErrorCountField());
  }

  @Test
  void testGetErrorFieldsField() {
    CsvInputMeta meta = new CsvInputMeta();
    assertNull(meta.getErrorFieldsField());
  }

  @Test
  void testGetErrorTextField() {
    CsvInputMeta meta = new CsvInputMeta();
    assertNull(meta.getErrorTextField());
  }

  @Test
  void testGetEscapeCharacter() {
    CsvInputMeta meta = new CsvInputMeta();
    assertNull(meta.getEscapeCharacter());
  }

  @Test
  void testGetNrHeaderLines() {
    CsvInputMeta meta = new CsvInputMeta();
    assertEquals(1, meta.getNrHeaderLines());
  }

  @Test
  void testHasHeader() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setHeaderPresent(true);
    assertTrue(meta.hasHeader());

    meta.setHeaderPresent(false);
    assertFalse(meta.hasHeader());
  }

  @Test
  void testGetFilePaths() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename("test.csv");
    IVariables variables = new Variables();

    String[] paths = meta.getFilePaths(variables);
    assertEquals(1, paths.length);
    assertEquals("test.csv", paths[0]);
  }

  @Test
  void testGetFilePathsWithVariable() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename("${FILE_PATH}");
    IVariables variables = new Variables();
    variables.setVariable("FILE_PATH", "/path/to/file.csv");

    String[] paths = meta.getFilePaths(variables);
    assertEquals(1, paths.length);
    assertEquals("/path/to/file.csv", paths[0]);
  }

  @Test
  void testGetFileFormatTypeNr() {
    CsvInputMeta meta = new CsvInputMeta();
    assertEquals(TextFileInputMeta.FILE_FORMAT_MIXED, meta.getFileFormatTypeNr());
  }

  @Test
  void testBreakInEnclosureAllowed() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setBreakInEnclosureAllowed(true);
    assertTrue(meta.isBreakInEnclosureAllowed());

    meta.setBreakInEnclosureAllowed(false);
    assertFalse(meta.isBreakInEnclosureAllowed());
  }

  @Test
  void testAllocate() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.allocate(5);

    assertNotNull(meta.getInputFields());
    assertEquals(5, meta.getInputFields().length);
  }

  @Test
  void testGetHeaderFileObjectWithValidFilename() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    // Use a file that should exist in most environments
    meta.setFilename("pom.xml");
    IVariables variables = new Variables();

    FileObject fileObject = meta.getHeaderFileObject(variables);
    // FileObject might be null if VFS fails, but the method should not throw
    // Just check it doesn't throw an exception
    assertNotNull(fileObject);
  }

  @Test
  void testGetHeaderFileObjectWithNonExistentFilename() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename("/nonexistent/path/to/file.csv");
    IVariables variables = new Variables();

    FileObject fileObject = meta.getHeaderFileObject(variables);
    // VFS may return a FileObject even for non-existent files, just verify no exception is thrown
    assertNotNull(fileObject);
  }

  @Test
  void testGetHeaderFileObjectWithVariable() throws Exception {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename("${FILE_PATH}");
    IVariables variables = new Variables();
    variables.setVariable("FILE_PATH", "pom.xml");

    FileObject fileObject = meta.getHeaderFileObject(variables);
    assertNotNull(fileObject);
  }

  @Test
  void testGettersAndSetters() {
    CsvInputMeta meta = new CsvInputMeta();

    meta.setFilename("test.csv");
    assertEquals("test.csv", meta.getFilename());

    meta.setFilenameField("filenameField");
    assertEquals("filenameField", meta.getFilenameField());

    meta.setIncludingFilename(true);
    assertTrue(meta.isIncludingFilename());

    meta.setRowNumField("rownum");
    assertEquals("rownum", meta.getRowNumField());

    meta.setHeaderPresent(false);
    assertFalse(meta.isHeaderPresent());

    meta.setDelimiter(";");
    assertEquals(";", meta.getDelimiter());

    meta.setEnclosure("'");
    assertEquals("'", meta.getEnclosure());

    meta.setBufferSize("10000");
    assertEquals("10000", meta.getBufferSize());

    meta.setLazyConversionActive(false);
    assertFalse(meta.isLazyConversionActive());

    meta.setAddResult(true);
    assertTrue(meta.isAddResult());

    meta.setRunningInParallel(true);
    assertTrue(meta.isRunningInParallel());

    meta.setEncoding("UTF-8");
    assertEquals("UTF-8", meta.getEncoding());

    meta.setNewlinePossibleInFields(true);
    assertTrue(meta.isNewlinePossibleInFields());

    meta.setSchemaDefinition("schema");
    assertEquals("schema", meta.getSchemaDefinition());

    meta.setIgnoreFields(true);
    assertTrue(meta.isIgnoreFields());

    TextFileInputField[] fields = new TextFileInputField[1];
    fields[0] = new TextFileInputField();
    meta.setInputFields(fields);
    assertEquals(1, meta.getInputFields().length);
  }
}
