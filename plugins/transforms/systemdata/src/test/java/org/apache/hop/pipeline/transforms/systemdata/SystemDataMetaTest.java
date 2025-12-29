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

package org.apache.hop.pipeline.transforms.systemdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

/** User: Dzmitry Stsiapanau Date: 1/20/14 Time: 3:04 PM */
class SystemDataMetaTest implements IInitializer<SystemDataMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  LoadSaveTester loadSaveTester;
  Class<SystemDataMeta> testMetaClass = SystemDataMeta.class;
  SystemDataMeta expectedSystemDataMeta;
  String expectedXML =
      "    <fields>\n"
          + "      <field>\n"
          + "        <name>hostname_real</name>\n"
          + "        <type>Hostname real</type>\n"
          + "        </field>\n"
          + "      <field>\n"
          + "        <name>hostname</name>\n"
          + "        <type>Hostname</type>\n"
          + "        </field>\n"
          + "      </fields>\n";

  @BeforeEach
  void setUp() throws Exception {
    expectedSystemDataMeta = new SystemDataMeta();
    expectedSystemDataMeta.allocate(2);
    String[] names = expectedSystemDataMeta.getFieldName();
    SystemDataTypes[] types = expectedSystemDataMeta.getFieldType();
    names[0] = "hostname_real";
    names[1] = "hostname";
    types[0] =
        SystemDataTypes.getTypeFromString(
            SystemDataTypes.TYPE_SYSTEM_INFO_HOSTNAME_REAL.getDescription());
    types[1] =
        SystemDataTypes.getTypeFromString(
            SystemDataTypes.TYPE_SYSTEM_INFO_HOSTNAME.getDescription());
  }

  @Test
  void testLoadXml() throws Exception {
    SystemDataMeta systemDataMeta = new SystemDataMeta();
    DocumentBuilderFactory documentBuilderFactory =
        XmlParserFactoryProducer.createSecureDocBuilderFactory();
    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    Node node = documentBuilder.parse(new InputSource(new StringReader(expectedXML)));
    systemDataMeta.loadXml(node, null);
    assertEquals(expectedSystemDataMeta, systemDataMeta);
  }

  @Test
  void testGetXml() throws Exception {
    String generatedXML = expectedSystemDataMeta.getXml();
    assertEquals(
        expectedXML.replaceAll("\n", "").replaceAll("\r", ""),
        generatedXML.replaceAll("\n", "").replaceAll("\r", ""));
  }

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes = Arrays.asList("fieldName", "fieldType");

    Map<String, String> getterMap =
        new HashMap<>() {
          {
            put("fieldName", "getFieldName");
            put("fieldType", "getFieldType");
          }
        };
    Map<String, String> setterMap =
        new HashMap<>() {
          {
            put("fieldName", "setFieldName");
            put("fieldType", "setFieldType");
          }
        };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), 5);

    IFieldLoadSaveValidator<SystemDataTypes[]> sdtArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<>(new SystemDataTypesLoadSaveValidator(), 5);

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put("fieldName", stringArrayLoadSaveValidator);
    attrValidatorMap.put("fieldType", sdtArrayLoadSaveValidator);

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(SystemDataMeta someMeta) {
    if (someMeta instanceof SystemDataMeta) {
      ((SystemDataMeta) someMeta).allocate(5);
    }
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class SystemDataTypesLoadSaveValidator
      implements IFieldLoadSaveValidator<SystemDataTypes> {
    final Random rand = new Random();

    @Override
    public SystemDataTypes getTestObject() {
      SystemDataTypes[] allTypes = SystemDataTypes.values();
      return allTypes[rand.nextInt(allTypes.length)];
    }

    @Override
    public boolean validateTestObject(SystemDataTypes testObject, Object actual) {
      if (!(actual instanceof SystemDataTypes)) {
        return false;
      }
      SystemDataTypes actualInput = (SystemDataTypes) actual;
      return (testObject.toString().equals(actualInput.toString()));
    }
  }
}
