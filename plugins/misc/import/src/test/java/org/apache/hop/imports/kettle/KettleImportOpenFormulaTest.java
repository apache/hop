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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** Kettle/PDI Formula steps import as OpenFormula and keep semicolon argument separators. */
class KettleImportOpenFormulaTest {

  private static final String ENTRY_TYPE = "org.apache.hop.imports.kettle.KettleImport$EntryType";

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void formulaStepBecomesOpenFormulaAndKeepsSemicolons() throws Exception {
    String source =
        """
        <transformation>
          <step>
            <name>Calc</name>
            <type>Formula</type>
            <formula>
              <field_name>label</field_name>
              <formula_string>IF([flag];"Y";"N")</formula_string>
              <value_type>String</value_type>
              <value_length>-1</value_length>
              <value_precision>-1</value_precision>
              <replace_field/>
            </formula>
            <GUI>
              <xloc>100</xloc>
              <yloc>100</yloc>
            </GUI>
          </step>
        </transformation>
        """;
    Document doc =
        XmlParserFactoryProducer.createSecureDocBuilderFactory()
            .newDocumentBuilder()
            .parse(new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)));
    // The importer walks the root element, the same way KettleImport does.
    processNode(doc, doc.getDocumentElement());

    String xml = toXml(doc);
    assertTrue(xml.contains(">OpenFormula<"), xml);
    assertTrue(xml.contains("IF([flag];\"Y\";\"N\")"), xml);
    assertFalse(xml.contains("IF([flag],\"Y\",\"N\")"), xml);

    Node transform =
        XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "transformation"), "transform");
    assertEquals("OpenFormula", XmlHandler.getTagValue(transform, "type"));
    Node formula = XmlHandler.getSubNode(XmlHandler.getSubNode(transform, "formulas"), "formula");
    if (formula == null) {
      formula = XmlHandler.getSubNode(transform, "formula");
    }
    assertEquals("IF([flag];\"Y\";\"N\")", XmlHandler.getTagValue(formula, "formula"));
    assertEquals(
        Integer.toString(IValueMeta.TYPE_STRING), XmlHandler.getTagValue(formula, "value_type"));
  }

  private static void processNode(Document doc, Node root) throws Exception {
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
    method.invoke(new KettleImport(), doc, root, other, 0);
  }

  private static String toXml(Document doc) throws Exception {
    Transformer transformer = XmlHandler.createSecureTransformerFactory().newTransformer();
    java.io.StringWriter writer = new java.io.StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));
    return writer.toString();
  }
}
