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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Kettle spells some step ids with a different case than Hop, for example {@code CsvInput} for
 * Hop's {@code CSVInput}. Hop runs the transform either way, but Hop GUI draws it with the
 * missing-plugin icon unless the imported type uses the registered spelling (issue #9084).
 */
class KettleImportTransformTypeCaseTest {

  private static final String ENTRY_TYPE = "org.apache.hop.imports.kettle.KettleImport$EntryType";

  private static final String[] HOP_IDS = {
    "CSVInput", "DynamicSqlRow", "ExecSql", "ExecSqlRow", "HttpPost"
  };

  private static final List<IPlugin> plugins = new ArrayList<>();

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
    for (String id : HOP_IDS) {
      IPlugin plugin = mock(IPlugin.class);
      when(plugin.getIds()).thenReturn(new String[] {id});
      when(plugin.getName()).thenReturn(id);
      when(plugin.matches(anyString()))
          .thenAnswer(invocation -> id.equalsIgnoreCase(invocation.getArgument(0)));
      PluginRegistry.getInstance().registerPlugin(TransformPluginType.class, plugin);
      plugins.add(plugin);
    }
  }

  @AfterAll
  static void tearDownAfterClass() {
    for (IPlugin plugin : plugins) {
      PluginRegistry.getInstance().removePlugin(TransformPluginType.class, plugin);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "CsvInput,CSVInput",
    "DynamicSQLRow,DynamicSqlRow",
    "ExecSQL,ExecSql",
    "ExecSQLRow,ExecSqlRow",
    "HTTPPOST,HttpPost"
  })
  void testKettleCaseBecomesRegisteredId(String kettleType, String hopType) throws Exception {
    assertEquals(hopType, importedType(kettleType));
  }

  @Test
  void testRegisteredIdIsKept() throws Exception {
    assertEquals("CSVInput", importedType("CSVInput"));
  }

  @Test
  void testUnknownTypeIsLeftAlone() throws Exception {
    assertEquals("SomeUnknownStep", importedType("SomeUnknownStep"));
  }

  /** Only the step type is aligned, not other elements that happen to hold the same text. */
  @Test
  void testTransformNameIsLeftAlone() throws Exception {
    Node transform = importStep("CsvInput", "CsvInput");
    assertEquals("CsvInput", XmlHandler.getTagValue(transform, "name"));
    assertEquals("CSVInput", XmlHandler.getTagValue(transform, "type"));
  }

  private static String importedType(String kettleType) throws Exception {
    return XmlHandler.getTagValue(importStep("read", kettleType), "type");
  }

  private static Node importStep(String name, String type) throws Exception {
    Document doc =
        XmlParserFactoryProducer.createSecureDocBuilderFactory()
            .newDocumentBuilder()
            .parse(
                new ByteArrayInputStream(
                    ("<transformation><step><name>"
                            + name
                            + "</name><type>"
                            + type
                            + "</type></step></transformation>")
                        .getBytes(StandardCharsets.UTF_8)));
    processNode(doc);
    return XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
  }

  private static void processNode(Document doc) throws Exception {
    Class<?> entryTypeClass = Class.forName(ENTRY_TYPE);
    Object other = null;
    for (Object constant : entryTypeClass.getEnumConstants()) {
      if ("OTHER".equals(constant.toString())) {
        other = constant;
      }
    }
    Method method =
        KettleImport.class.getDeclaredMethod(
            "processNode", Document.class, Node.class, entryTypeClass, int.class);
    method.setAccessible(true);
    method.invoke(new KettleImport(), doc, doc, other, 0);
  }
}
