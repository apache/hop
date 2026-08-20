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

package org.apache.hop.core.xml;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.SchemaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.xml.sax.SAXException;

/** Unit test for {@link XmlParserFactoryProducer} */
class XmlUtilsTest {
  @Test
  void secureFeatureEnabledAfterDocBuilderFactoryCreation() throws Exception {
    DocumentBuilderFactory documentBuilderFactory =
        XmlParserFactoryProducer.createSecureDocBuilderFactory();

    assertTrue(documentBuilderFactory.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
  }

  @Test
  void secureFeatureEnabledAfterSAXParserFactoryCreation() throws Exception {
    SAXParserFactory saxParserFactory = XmlParserFactoryProducer.createSecureSAXParserFactory();

    assertTrue(saxParserFactory.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
  }

  @Test
  void secureFeatureEnabledAfterSchemaFactoryCreation() throws Exception {
    SchemaFactory schemaFactory =
        XmlParserFactoryProducer.createSecureSchemaFactory(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    assertTrue(schemaFactory.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
  }

  // The accessExternalDTD / accessExternalSchema values cannot be read back from Xerces'
  // XMLSchemaFactory -- setProperty accepts them but getProperty rejects them as unrecognized -- so
  // the two tests below assert their effect instead.

  @Test
  void secureSchemaFactoryRefusesRemoteSchemaReference(@TempDir Path tempDir) throws Exception {
    // The host is never contacted: access is refused by the accessExternalSchema restriction
    // before any network I/O is attempted.
    Path schema = tempDir.resolve("remote-include.xsd");
    Files.writeString(
        schema,
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            + "<xs:include schemaLocation=\"http://hop.apache.org.invalid/stolen.xsd\"/>"
            + "</xs:schema>");

    SchemaFactory schemaFactory =
        XmlParserFactoryProducer.createSecureSchemaFactory(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    SAXException e =
        assertThrows(SAXException.class, () -> schemaFactory.newSchema(schema.toFile()));
    assertTrue(
        e.getMessage().contains("access is not allowed"),
        "expected an access restriction failure but got: " + e.getMessage());
  }

  @Test
  void secureSchemaFactoryStillResolvesLocalSchemaReference(@TempDir Path tempDir)
      throws Exception {
    // Restricting external access must not break multi-file schemas on the local file system.
    Files.writeString(
        tempDir.resolve("included.xsd"),
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            + "<xs:element name=\"included\" type=\"xs:string\"/>"
            + "</xs:schema>");
    File including = tempDir.resolve("including.xsd").toFile();
    Files.writeString(
        including.toPath(),
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            + "<xs:include schemaLocation=\"included.xsd\"/>"
            + "<xs:element name=\"root\" type=\"xs:string\"/>"
            + "</xs:schema>");

    SchemaFactory schemaFactory =
        XmlParserFactoryProducer.createSecureSchemaFactory(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    assertDoesNotThrow(() -> schemaFactory.newSchema(including));
  }
}
