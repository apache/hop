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

package org.apache.hop.pipeline.transforms.jsonoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hop.TestUtilities;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.TransformRowsCollector;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.rowgenerator.GeneratorField;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JsonOutputTest {

  private static final String EXPECTED_JSON =
      "{\"data\":[{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"}]}";

  /** Creates a row generator transform for this class.. */
  private TransformMeta createRowGeneratorTransform(String name, PluginRegistry registry) {

    // Default the name if it is empty
    String testFileOutputName = (Utils.isEmpty(name) ? "generate rows" : name);

    // create the RowGenerator and Transform Meta
    RowGeneratorMeta rowGeneratorMeta = new RowGeneratorMeta();
    String rowGeneratorPid = registry.getPluginId(TransformPluginType.class, rowGeneratorMeta);
    TransformMeta generateRowsTransform =
        new TransformMeta(rowGeneratorPid, testFileOutputName, rowGeneratorMeta);

    // Set the field names, types and values
    rowGeneratorMeta
        .getFields()
        .addAll(
            Arrays.asList(
                new GeneratorField("Id", "Integer", "", -1, -1, "", "", "", "1", false),
                new GeneratorField("State", "String", "", -1, -1, "", "", "", "Florida", false),
                new GeneratorField("City", "String", "", -1, -1, "", "", "", "Orlando", false)));
    rowGeneratorMeta.setRowLimit("10");

    // return the transform meta
    return generateRowsTransform;
  }

  /** Create a dummy transform for this class. */
  private TransformMeta createDummyTransform(String name, PluginRegistry registry) {
    // Create a dummy transform 1 and add it to the tranMeta
    String dummyTransformName = "dummy transform";
    DummyMeta dm1 = new DummyMeta();
    String dummyPid1 = registry.getPluginId(TransformPluginType.class, dm1);
    return new TransformMeta(dummyPid1, dummyTransformName, dm1);
  }

  /**
   * Create result data for test case 1. Each Object array in element in list should mirror the data
   * written by the row generator created by the createRowGenerator method.
   *
   * @return list of metadata/data couples of how the result should look like.
   */
  public List<RowMetaAndData> createResultData1() {
    List<RowMetaAndData> list = new ArrayList<>();

    IRowMeta rowMetaInterface = createResultRowMeta();

    Object[] r1 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r2 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r3 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r4 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r5 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r6 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r7 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r8 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r9 = new Object[] {1L, "Orlando", "Florida"};
    Object[] r10 = new Object[] {1L, "Orlando", "Florida"};

    list.add(new RowMetaAndData(rowMetaInterface, r1));
    list.add(new RowMetaAndData(rowMetaInterface, r2));
    list.add(new RowMetaAndData(rowMetaInterface, r3));
    list.add(new RowMetaAndData(rowMetaInterface, r4));
    list.add(new RowMetaAndData(rowMetaInterface, r5));
    list.add(new RowMetaAndData(rowMetaInterface, r6));
    list.add(new RowMetaAndData(rowMetaInterface, r7));
    list.add(new RowMetaAndData(rowMetaInterface, r8));
    list.add(new RowMetaAndData(rowMetaInterface, r9));
    list.add(new RowMetaAndData(rowMetaInterface, r10));
    return list;
  }

  /** Creates a IRowMeta with a IValueMeta with the name "filename". */
  public IRowMeta createIRowMeta() {
    IRowMeta rowMetaInterface = new RowMeta();

    IValueMeta[] valuesMeta = {
      new ValueMetaString("filename"),
    };
    for (IValueMeta iValueMeta : valuesMeta) {
      rowMetaInterface.addValueMeta(iValueMeta);
    }

    return rowMetaInterface;
  }

  /** Creates data... Will add more as I figure what the data is. */
  public List<RowMetaAndData> createData() {
    List<RowMetaAndData> list = new ArrayList<>();
    IRowMeta rowMetaInterface = createIRowMeta();
    Object[] r1 = new Object[] {};
    list.add(new RowMetaAndData(rowMetaInterface, r1));
    return list;
  }

  /**
   * Creates a row meta interface for the fields that are defined by performing a getFields and by
   * checking "Result filenames - Add filenames to result from "Text File Input" dialog.
   */
  public IRowMeta createResultRowMeta() {
    IRowMeta rowMetaInterface = new RowMeta();

    IValueMeta[] valuesMeta = {
      new ValueMetaInteger("Id"), new ValueMetaString("State"), new ValueMetaString("City")
    };

    for (IValueMeta iValueMeta : valuesMeta) {
      rowMetaInterface.addValueMeta(iValueMeta);
    }

    return rowMetaInterface;
  }

  private TransformMeta createJsonOutputTransform(
      String name, String jsonFileName, PluginRegistry registry) {

    // Create a Text File Output transform
    String testFileOutputName = name;
    JsonOutputMeta jsonOutputMeta = new JsonOutputMeta();
    String textFileInputPid = registry.getPluginId(TransformPluginType.class, jsonOutputMeta);
    TransformMeta jsonOutputTransform =
        new TransformMeta(textFileInputPid, testFileOutputName, jsonOutputMeta);

    // initialize the fields
    List<JsonOutputField> fields = new ArrayList<>();

    // populate the fields
    // it is important that the setPosition(int)
    // is invoked with the correct position as
    // we are testing the reading of a delimited file.
    JsonOutputField outputField1 = new JsonOutputField();
    outputField1.setFieldName("id");
    outputField1.setElementName("id");
    fields.add(outputField1);

    JsonOutputField outputField2 = new JsonOutputField();
    outputField2.setFieldName("state");
    outputField2.setElementName("state");
    fields.add(outputField2);

    JsonOutputField outputField3 = new JsonOutputField();
    outputField3.setFieldName("city");
    outputField3.setElementName("city");
    fields.add(outputField3);

    // call this to allocate the number of fields
    jsonOutputMeta.setOutputFields(fields);

    // set meta properties- these were determined by running Spoon
    // and setting up the transformation we are setting up here.
    // i.e. - the dialog told me what I had to set to avoid
    // NPEs during the transformation.

    // We need a file name so we will generate a temp file
    jsonOutputMeta.setOperationType(JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE);
    jsonOutputMeta.setOutputValue("data");
    jsonOutputMeta.setFileName(jsonFileName);
    jsonOutputMeta.setExtension("js");
    jsonOutputMeta.setNrRowsInBloc("0"); // a single "data" contains an array of all records
    jsonOutputMeta.setJsonBloc("data");

    return jsonOutputTransform;
  }

  @Test
  void test() throws Exception {
    HopEnvironment.init();

    // Create a new transformation...
    //
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("testJsonOutput");
    PluginRegistry registry = PluginRegistry.getInstance();

    // create a row generator transform
    TransformMeta rowGeneratorTransform =
        createRowGeneratorTransform("Create rows for testJsonOutput1", registry);
    pipelineMeta.addTransform(rowGeneratorTransform);

    // create the json output transform
    // but first lets get a filename
    String jsonFileName = TestUtilities.createEmptyTempFile("testJsonOutput1_");
    TransformMeta jsonOutputTransform =
        createJsonOutputTransform("json output transform", jsonFileName, registry);
    pipelineMeta.addTransform(jsonOutputTransform);

    // create a PipelineHopMeta for jsonOutputTransform and add it to the pipelineMeta
    PipelineHopMeta hopRowGeneratorOutputTextFile =
        new PipelineHopMeta(rowGeneratorTransform, jsonOutputTransform);
    pipelineMeta.addPipelineHop(hopRowGeneratorOutputTextFile);

    // Create a dummy transform and add it to the tranMeta
    String dummyTransformName = "dummy transform";
    TransformMeta dummyTransform = createDummyTransform(dummyTransformName, registry);
    pipelineMeta.addTransform(dummyTransform);

    // create a PipelineHopMeta for the
    PipelineHopMeta hopOutputJsonDummyTransform =
        new PipelineHopMeta(jsonOutputTransform, dummyTransform);
    pipelineMeta.addPipelineHop(hopOutputJsonDummyTransform);

    // Now execute the transformation...
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();

    // Create a row collector and add it to the dummy transform interface
    IEngineComponent dummyITransform = pipeline.findComponent(dummyTransformName, 0);
    TransformRowsCollector dummyRowCollector = new TransformRowsCollector();
    dummyITransform.addRowListener(dummyRowCollector);

    pipeline.startThreads();
    pipeline.waitUntilFinished();

    // get the results and return it
    File outputFile = new File(jsonFileName + ".js");
    String jsonStructure = FileUtils.readFileToString(outputFile);
    assertTrue(jsonEquals(EXPECTED_JSON, jsonStructure));
  }

  @Test
  void testNpeIsNotThrownOnNullInput() throws Exception {
    TransformMockHelper<JsonOutputMeta, JsonOutputData> mockHelper =
        new TransformMockHelper<>("jsonOutput", JsonOutputMeta.class, JsonOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getTransform()).thenReturn(new JsonOutputMeta());

    JsonOutput transform =
        new JsonOutput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform = spy(transform);

    doReturn(null).when(transform).getRow();

    try {
      transform.processRow();
    } finally {
      mockHelper.cleanUp();
    }
  }

  @Test
  void testEmptyDoesntWriteToFile() throws Exception {
    TransformMockHelper<JsonOutputMeta, JsonOutputData> mockHelper =
        new TransformMockHelper<>("jsonOutput", JsonOutputMeta.class, JsonOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getTransform()).thenReturn(new JsonOutputMeta());

    JsonOutputData transformData = new JsonOutputData();
    transformData.writeToFile = true;
    JsonOutput transform =
        new JsonOutput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            transformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform = spy(transform);

    doReturn(null).when(transform).getRow();
    doReturn(true).when(transform).openNewFile();
    doReturn(true).when(transform).closeFile();

    try {
      transform.processRow();
      verify(transform, times(0)).openNewFile();
      verify(transform, times(0)).closeFile();
    } finally {
      mockHelper.cleanUp();
    }
  }

  @Test
  void testWriteToFile() throws Exception {
    TransformMockHelper<JsonOutputMeta, JsonOutputData> mockHelper =
        new TransformMockHelper<>("jsonOutput", JsonOutputMeta.class, JsonOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.getTransform()).thenReturn(new JsonOutputMeta());

    JsonOutputData transformData = new JsonOutputData();
    transformData.writeToFile = true;
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("key", "value");
    transformData.ja.add(jsonObject);
    transformData.writer = mock(Writer.class);

    JsonOutput transform =
        new JsonOutput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            transformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform = spy(transform);

    doReturn(null).when(transform).getRow();
    doReturn(true).when(transform).openNewFile();
    doReturn(true).when(transform).closeFile();
    doNothing().when(transformData.writer).write(anyString());

    try {
      transform.processRow();
      verify(transform).openNewFile();
      verify(transform).closeFile();
    } finally {
      mockHelper.cleanUp();
    }
  }

  /**
   * Reproduces #2958: when the number of input rows is not a multiple of "Nr. rows in a block", the
   * last (partial) block used to be emitted on a row where every input field was null. Each output
   * row must carry the input field values of the row that closed its block.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE,
        JsonOutputMeta.OPERATION_TYPE_BOTH,
      })
  void testPartialLastBlockKeepsInputFields(String operationType) throws Exception {
    HopEnvironment.init();

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("testJsonOutputPartialBlock");
    PluginRegistry registry = PluginRegistry.getInstance();

    // 10 rows, blocks of 3: 3 full blocks + 1 partial block of a single row
    //
    TransformMeta rowGeneratorTransform = createRowGeneratorTransform("generate rows", registry);
    pipelineMeta.addTransform(rowGeneratorTransform);

    String jsonFileName = TestUtilities.createEmptyTempFile("testJsonOutputPartialBlock_");
    TransformMeta jsonOutputTransform =
        createJsonOutputTransform("json output transform", jsonFileName, registry);
    JsonOutputMeta jsonOutputMeta = (JsonOutputMeta) jsonOutputTransform.getTransform();
    jsonOutputMeta.setOperationType(operationType);
    jsonOutputMeta.setOutputValue("json");
    jsonOutputMeta.setNrRowsInBloc("3");
    pipelineMeta.addTransform(jsonOutputTransform);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(rowGeneratorTransform, jsonOutputTransform));

    String dummyTransformName = "dummy transform";
    TransformMeta dummyTransform = createDummyTransform(dummyTransformName, registry);
    pipelineMeta.addTransform(dummyTransform);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(jsonOutputTransform, dummyTransform));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    IEngineComponent dummyITransform = pipeline.findComponent(dummyTransformName, 0);
    TransformRowsCollector dummyRowCollector = new TransformRowsCollector();
    dummyITransform.addRowListener(dummyRowCollector);
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors(), "pipeline should run without errors");

    List<RowMetaAndData> rows = dummyRowCollector.getRowsWritten();
    assertEquals(4, rows.size(), "one output row per block, including the partial last block");

    int[] expectedBlockSizes = {3, 3, 3, 1};
    for (int i = 0; i < rows.size(); i++) {
      RowMetaAndData row = rows.get(i);
      IRowMeta rowMeta = row.getRowMeta();
      assertEquals(4, rowMeta.size());

      // The input fields of the row that closed the block must be present on the output row
      //
      assertEquals(1L, row.getInteger("Id", -1L), "row " + i + ": Id");
      assertEquals("Florida", row.getString("State", null), "row " + i + ": State");
      assertEquals("Orlando", row.getString("City", null), "row " + i + ": City");

      String json = row.getString("json", null);
      assertNotNull(json, "row " + i + ": json");
      JsonNode block = HopJson.newMapper().readTree(json).get("data");
      assertNotNull(block, "row " + i + ": json block 'data'");
      assertEquals(expectedBlockSizes[i], block.size(), "row " + i + ": block size");
    }
  }

  /**
   * Write-to-file mode with "Nr. rows in a block": every block goes to its own numbered file, the
   * parent folder is created on demand, the files are added to the result and all field types are
   * rendered in the JSON.
   */
  @Test
  void testWriteToFileOneFilePerBlock() throws Exception {
    HopEnvironment.init();

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("testJsonOutputFilePerBlock");
    PluginRegistry registry = PluginRegistry.getInstance();

    // Other tests in this module register only a subset of the value meta plugins; make sure the
    // types used below are known regardless of the test order.
    //
    for (Class<?> valueMetaClass :
        new Class<?>[] {
          ValueMetaBoolean.class,
          ValueMetaNumber.class,
          ValueMetaBigNumber.class,
          ValueMetaDate.class
        }) {
      registry.registerPluginClass(
          valueMetaClass.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    }

    RowGeneratorMeta rowGeneratorMeta = new RowGeneratorMeta();
    rowGeneratorMeta
        .getFields()
        .addAll(
            Arrays.asList(
                new GeneratorField("Id", "Integer", "", -1, -1, "", "", "", "1", false),
                new GeneratorField("State", "String", "", -1, -1, "", "", "", "Florida", false),
                new GeneratorField("Flag", "Boolean", "", -1, -1, "", "", "", "true", false),
                new GeneratorField("Ratio", "Number", "#.#", -1, -1, "", ".", "", "1.5", false),
                new GeneratorField(
                    "Amount", "BigNumber", "#.#", -1, -1, "", ".", "", "12345.6", false),
                new GeneratorField(
                    "Day", "Date", "yyyy-MM-dd", -1, -1, "", "", "", "2023-05-23", false)));
    rowGeneratorMeta.setRowLimit("7");
    TransformMeta rowGeneratorTransform =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, rowGeneratorMeta),
            "generate rows",
            rowGeneratorMeta);
    pipelineMeta.addTransform(rowGeneratorTransform);

    // Write into a folder that does not exist yet
    //
    File baseFolder = new File(TestUtilities.createEmptyTempFile("testJsonOutputFilePerBlock_"));
    assertTrue(baseFolder.delete());
    String jsonFileName = new File(baseFolder, "out/blocks").getPath();

    JsonOutputMeta jsonOutputMeta = new JsonOutputMeta();
    List<JsonOutputField> fields = new ArrayList<>();
    for (String name : new String[] {"Id", "State", "Flag", "Ratio", "Amount", "Day"}) {
      JsonOutputField field = new JsonOutputField();
      field.setFieldName(name);
      field.setElementName(name.toLowerCase());
      fields.add(field);
    }
    jsonOutputMeta.setOutputFields(fields);
    jsonOutputMeta.setOperationType(JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE);
    jsonOutputMeta.setFileName(jsonFileName);
    jsonOutputMeta.setExtension("json");
    jsonOutputMeta.setJsonBloc("data");
    jsonOutputMeta.setNrRowsInBloc("3");
    jsonOutputMeta.setEncoding("UTF-8");
    jsonOutputMeta.setCreateParentFolder(true);
    jsonOutputMeta.setAddToResult(true);
    TransformMeta jsonOutputTransform =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, jsonOutputMeta),
            "json output transform",
            jsonOutputMeta);
    pipelineMeta.addTransform(jsonOutputTransform);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(rowGeneratorTransform, jsonOutputTransform));

    String dummyTransformName = "dummy transform";
    TransformMeta dummyTransform = createDummyTransform(dummyTransformName, registry);
    pipelineMeta.addTransform(dummyTransform);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(jsonOutputTransform, dummyTransform));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    IEngineComponent dummyITransform = pipeline.findComponent(dummyTransformName, 0);
    TransformRowsCollector dummyRowCollector = new TransformRowsCollector();
    dummyITransform.addRowListener(dummyRowCollector);
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors(), "pipeline should run without errors");

    // In write-to-file mode the input rows are passed through unchanged
    //
    assertEquals(7, dummyRowCollector.getRowsWritten().size());
    assertEquals(6, dummyRowCollector.getRowsWritten().get(0).getRowMeta().size());

    // 7 rows in blocks of 3: files _0, _1 and _2 with 3, 3 and 1 rows
    //
    int[] expectedBlockSizes = {3, 3, 1};
    ObjectMapper mapper = HopJson.newMapper();
    for (int i = 0; i < expectedBlockSizes.length; i++) {
      File outputFile = new File(jsonFileName + "_" + i + ".json");
      assertTrue(outputFile.exists(), "file " + outputFile + " should exist");
      JsonNode block = mapper.readTree(FileUtils.readFileToString(outputFile, "UTF-8")).get("data");
      assertNotNull(block, "file " + i + ": json block 'data'");
      assertEquals(expectedBlockSizes[i], block.size(), "file " + i + ": block size");
      JsonNode first = block.get(0);
      assertEquals(1L, first.get("id").asLong());
      assertEquals("Florida", first.get("state").asText());
      assertTrue(first.get("flag").asBoolean());
      assertEquals(1.5d, first.get("ratio").asDouble(), 0.0001);
      assertEquals(new BigDecimal("12345.6"), first.get("amount").decimalValue());
      assertEquals("2023-05-23", first.get("day").asText().substring(0, 10));
    }
    assertFalse(new File(jsonFileName + "_3.json").exists(), "no fourth (empty) file");

    // The written files are registered in the result
    //
    List<String> resultFileNames = new ArrayList<>();
    for (ResultFile resultFile : pipeline.getResult().getResultFiles().values()) {
      resultFileNames.add(resultFile.getFile().getName().getBaseName());
    }
    assertEquals(3, resultFileNames.size(), "result files: " + resultFileNames);
    assertTrue(resultFileNames.contains("blocks_0.json"));
    assertTrue(resultFileNames.contains("blocks_2.json"));
  }

  /** compare json (deep equals ignoring order) */
  protected boolean jsonEquals(String json1, String json2) throws Exception {
    ObjectMapper om = HopJson.newMapper();
    JsonNode parsedJson1 = om.readTree(json1);
    JsonNode parsedJson2 = om.readTree(json2);
    return parsedJson1.equals(parsedJson2);
  }
}
