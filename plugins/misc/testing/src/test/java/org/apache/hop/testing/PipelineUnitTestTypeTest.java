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

package org.apache.hop.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.testing.transforms.exectests.ExecuteTestsMeta;
import org.apache.hop.testing.util.DataSetConst;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;

class PipelineUnitTestTypeTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void legacyCodesRoundTripThroughDialogLabels() {
    String development = DataSetConst.getTestTypeDescription(DataSetConst.TEST_TYPE_DEVELOPMENT);
    String unitTest = DataSetConst.getTestTypeDescription(DataSetConst.TEST_TYPE_UNIT_TEST);

    assertEquals(development, DataSetConst.getTestTypeDescription(null));
    assertEquals(development, DataSetConst.getTestTypeDescription(""));
    assertEquals(DataSetConst.TEST_TYPE_DEVELOPMENT, DataSetConst.getTestTypeForDescription(null));
    assertEquals(DataSetConst.TEST_TYPE_DEVELOPMENT, DataSetConst.getTestTypeForDescription(""));
    assertEquals(
        DataSetConst.TEST_TYPE_DEVELOPMENT, DataSetConst.getTestTypeForDescription(development));
    assertEquals(
        DataSetConst.TEST_TYPE_DEVELOPMENT,
        DataSetConst.getTestTypeForDescription(development.toLowerCase(Locale.ROOT)));
    assertEquals(
        DataSetConst.TEST_TYPE_UNIT_TEST, DataSetConst.getTestTypeForDescription(unitTest));
    assertEquals(
        DataSetConst.TEST_TYPE_UNIT_TEST,
        DataSetConst.getTestTypeForDescription(unitTest.toUpperCase(Locale.ROOT)));

    assertEquals("Smoke", DataSetConst.getTestTypeDescription("Smoke"));
    assertEquals("Smoke", DataSetConst.getTestTypeForDescription("Smoke"));
    assertEquals("  ", DataSetConst.getTestTypeForDescription("  "));
  }

  @Test
  void unsetTypeRunsEveryTestAndASetTypeMatchesExactly() {
    assertTrue(DataSetConst.matchesTestType(null, "Smoke"));
    assertTrue(DataSetConst.matchesTestType(null, null));
    assertTrue(DataSetConst.matchesTestType("Smoke", "Smoke"));
    assertTrue(
        DataSetConst.matchesTestType(
            DataSetConst.TEST_TYPE_UNIT_TEST, DataSetConst.TEST_TYPE_UNIT_TEST));
    assertFalse(DataSetConst.matchesTestType("Smoke", DataSetConst.TEST_TYPE_UNIT_TEST));
    assertFalse(DataSetConst.matchesTestType("Smoke", null));
    assertFalse(DataSetConst.matchesTestType("", "Smoke"));
    assertFalse(DataSetConst.matchesTestType("Unit test", DataSetConst.TEST_TYPE_UNIT_TEST));
  }

  @Test
  void newUnitTestDefaultsToDevelopment() {
    assertEquals(DataSetConst.TEST_TYPE_DEVELOPMENT, new PipelineUnitTest().getType());
  }

  @Test
  void descriptionsListBuiltInTypesAndProjectTypes() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    IHopMetadataSerializer<PipelineUnitTest> serializer =
        provider.getSerializer(PipelineUnitTest.class);
    save(serializer, "dev", DataSetConst.TEST_TYPE_DEVELOPMENT);
    save(serializer, "unit", DataSetConst.TEST_TYPE_UNIT_TEST);
    save(serializer, "smoke", "Smoke");
    save(serializer, "smoke-again", "Smoke");
    save(serializer, "regression", "Regression");
    save(serializer, "blank", "");

    String[] items = DataSetConst.getTestTypeDescriptions(provider);

    assertEquals(DataSetConst.getTestTypeDescription(DataSetConst.TEST_TYPE_DEVELOPMENT), items[0]);
    assertEquals(DataSetConst.getTestTypeDescription(DataSetConst.TEST_TYPE_UNIT_TEST), items[1]);
    assertEquals(List.of(items[0], items[1], "Regression", "Smoke"), Arrays.asList(items));
    assertEquals(2, DataSetConst.getTestTypeDescriptions().length);
    assertEquals(2, DataSetConst.getTestTypeDescriptions(null).length);
  }

  @Test
  void descriptionsStayUsableWhenMetadataCannotBeListed() throws Exception {
    IHopMetadataProvider broken = mock(IHopMetadataProvider.class);
    when(broken.getSerializer(PipelineUnitTest.class)).thenThrow(new HopException("nope"));

    assertEquals(2, DataSetConst.getTestTypeDescriptions(broken).length);
  }

  @Test
  @SuppressWarnings("unchecked")
  void descriptionsSkipAUnitTestThatFailsToLoad() throws Exception {
    IHopMetadataSerializer<PipelineUnitTest> serializer = mock(IHopMetadataSerializer.class);
    when(serializer.listObjectNames()).thenReturn(List.of("ok", "bad"));
    PipelineUnitTest ok = new PipelineUnitTest();
    ok.setName("ok");
    ok.setType("Smoke");
    when(serializer.load("ok")).thenReturn(ok);
    when(serializer.load("bad")).thenThrow(new HopException("bad"));
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    when(provider.getSerializer(PipelineUnitTest.class)).thenReturn(serializer);

    String[] items = DataSetConst.getTestTypeDescriptions(provider);

    assertEquals(3, items.length);
    assertEquals("Smoke", items[2]);
  }

  @Test
  void legacyJsonKeepsStoredCodesAndCustomTypesAreStoredAsEntered(@TempDir Path folder)
      throws Exception {
    writeUnitTest(folder, "legacy-unit", DataSetConst.TEST_TYPE_UNIT_TEST);
    writeUnitTest(folder, "legacy-dev", DataSetConst.TEST_TYPE_DEVELOPMENT);

    JsonMetadataSerializer<PipelineUnitTest> serializer = serializer(folder);

    PipelineUnitTest legacyUnit = serializer.load("legacy-unit");
    PipelineUnitTest legacyDev = serializer.load("legacy-dev");
    assertEquals(DataSetConst.TEST_TYPE_UNIT_TEST, legacyUnit.getType());
    assertEquals(DataSetConst.TEST_TYPE_DEVELOPMENT, legacyDev.getType());

    serializer.save(legacyUnit);
    String savedLegacy = read(serializer.calculateFilename("legacy-unit"));
    assertTrue(withoutSpaces(savedLegacy).contains("\"test_type\":\"UNIT_TEST\""));
    assertFalse(
        savedLegacy.contains(
            DataSetConst.getTestTypeDescription(DataSetConst.TEST_TYPE_UNIT_TEST)));

    PipelineUnitTest custom = new PipelineUnitTest();
    custom.setName("custom");
    custom.setType("Smoke");
    serializer.save(custom);

    assertEquals("Smoke", serializer.load("custom").getType());
    String savedCustom = read(serializer.calculateFilename("custom"));
    assertTrue(withoutSpaces(savedCustom).contains("\"test_type\":\"Smoke\""));
    assertFalse(withoutSpaces(savedCustom).contains("\"test_type\":\"DEVELOPMENT\""));
  }

  @Test
  void executeTestsKeepsLegacyAndCustomTypesInXml() throws Exception {
    assertNull(loadExecuteTests(null).getTypeToExecute());
    assertEquals(
        DataSetConst.TEST_TYPE_DEVELOPMENT, loadExecuteTests("DEVELOPMENT").getTypeToExecute());
    assertEquals(
        DataSetConst.TEST_TYPE_UNIT_TEST, loadExecuteTests("UNIT_TEST").getTypeToExecute());

    ExecuteTestsMeta smoke = loadExecuteTests("Smoke");
    assertEquals("Smoke", smoke.getTypeToExecute());
    String xml = smoke.getXml();
    assertTrue(xml.contains("<type_to_execute>Smoke</type_to_execute>"));

    Node copyNode = XmlHandler.loadXmlString("<transform>" + xml + "</transform>", "transform");
    ExecuteTestsMeta copy =
        XmlMetadataUtil.deSerializeFromXml(
            copyNode, ExecuteTestsMeta.class, new MemoryMetadataProvider());
    assertEquals("Smoke", copy.getTypeToExecute());
  }

  private static void save(
      IHopMetadataSerializer<PipelineUnitTest> serializer, String name, String type)
      throws HopException {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.setName(name);
    unitTest.setType(type);
    serializer.save(unitTest);
  }

  @SuppressWarnings("unchecked")
  private static JsonMetadataSerializer<PipelineUnitTest> serializer(Path folder)
      throws HopException {
    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            folder.toString(),
            Variables.getADefaultVariableSpace());
    return (JsonMetadataSerializer<PipelineUnitTest>)
        provider.getSerializer(PipelineUnitTest.class);
  }

  private static void writeUnitTest(Path folder, String name, String testType) throws Exception {
    String directory = folder.resolve("unit-test").toString();
    HopVfs.getFileObject(directory).createFolder();
    String filename = directory + "/" + name + ".json";
    String json = "{\"name\":\"" + name + "\",\"test_type\":\"" + testType + "\"}";
    try (OutputStream out = HopVfs.getOutputStream(filename, false)) {
      out.write(json.getBytes(UTF_8));
    }
  }

  private static String read(String filename) throws Exception {
    try (InputStream in = HopVfs.getInputStream(filename)) {
      return new String(in.readAllBytes(), UTF_8);
    }
  }

  private static String withoutSpaces(String value) {
    return value.replace(" ", "");
  }

  private static ExecuteTestsMeta loadExecuteTests(String typeToExecute) throws Exception {
    String xml =
        "<transform>"
            + (typeToExecute == null
                ? ""
                : "<type_to_execute>" + typeToExecute + "</type_to_execute>")
            + "</transform>";
    Node node = XmlHandler.loadXmlString(xml, "transform");
    return XmlMetadataUtil.deSerializeFromXml(
        node, ExecuteTestsMeta.class, new MemoryMetadataProvider());
  }
}
