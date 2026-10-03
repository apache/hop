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

package org.apache.hop.metadata.serializer.xml;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.junit.jupiter.api.Test;

class XmlMetadataSchemaTest {

  public enum TestColor {
    RED,
    GREEN,
    BLUE
  }

  public enum TestStatusCode implements IEnumHasCode {
    ACTIVE("A"),
    INACTIVE("I");

    private final String code;

    TestStatusCode(String code) {
      this.code = code;
    }

    @Override
    public String getCode() {
      return code;
    }
  }

  @Getter
  @Setter
  public static class TestItem {
    @HopMetadataProperty private String itemId;
    @HopMetadataProperty private int quantity;
  }

  @Getter
  @Setter
  public static class InlinedAddress {
    @HopMetadataProperty private String street;
    @HopMetadataProperty private String city;
  }

  @Getter
  @Setter
  @HopMetadataWrapper(tag = "customer_record")
  public static class CustomerRecord {
    @HopMetadataProperty(key = "full_name")
    private String name;

    @HopMetadataProperty private boolean active;

    @HopMetadataProperty private int age;

    @HopMetadataProperty private double balance;

    @HopMetadataProperty private Date birthDate;

    @HopMetadataProperty private TestColor favoriteColor;

    @HopMetadataProperty(storeWithCode = true)
    private TestStatusCode statusCode;

    @HopMetadataProperty(inline = true)
    private InlinedAddress address;

    @HopMetadataProperty(groupKey = "items", key = "item")
    private List<TestItem> items = new ArrayList<>();

    @HopMetadataProperty(key = "tag")
    private List<String> tags = new ArrayList<>();
  }

  @Test
  void testGenerateAndValidateSchema() throws Exception {
    String xsd = XmlMetadataSchema.generateSchema(CustomerRecord.class);
    assertNotNull(xsd);
    assertTrue(xsd.contains("xs:schema"));
    assertTrue(xsd.contains("name=\"customer_record\""));
    assertTrue(xsd.contains("name=\"full_name\""));
    assertTrue(xsd.contains("name=\"HopBoolean\""));
    assertTrue(xsd.contains("name=\"TestColorEnum\""));
    assertTrue(xsd.contains("name=\"TestStatusCodeCodeEnum\""));
    assertTrue(xsd.contains("value=\"A\""));
    assertTrue(xsd.contains("value=\"I\""));
    assertTrue(xsd.contains("name=\"street\""));
    assertTrue(xsd.contains("name=\"city\""));
    assertTrue(xsd.contains("name=\"items\""));
    assertTrue(xsd.contains("name=\"item\""));
    assertTrue(xsd.contains("name=\"tag\""));

    // Verify that the generated XSD is well-formed and valid W3C XML Schema
    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(xsd)));
    assertNotNull(schema);

    // Verify that valid XML validates against this schema
    String xml =
        """
        <customer_record>
          <full_name>John Doe</full_name>
          <active>Y</active>
          <age>42</age>
          <balance>123.45</balance>
          <birthDate>2020/01/01 10:00:00.000</birthDate>
          <favoriteColor>BLUE</favoriteColor>
          <statusCode>A</statusCode>
          <street>Main Street 1</street>
          <city>Brussels</city>
          <items>
            <item>
              <itemId>ITM-1</itemId>
              <quantity>5</quantity>
            </item>
          </items>
          <tag>vip</tag>
          <tag>loyal</tag>
        </customer_record>
        """;

    Validator validator = schema.newValidator();
    assertDoesNotThrow(() -> validator.validate(new StreamSource(new StringReader(xml))));

    // Verify out-of-order elements also validate with flexibleElementOrder
    String outOfOrderXml =
        """
        <customer_record>
          <active>N</active>
          <full_name>Jane Doe</full_name>
          <city>Ghent</city>
          <street>Kouter 1</street>
          <age>30</age>
        </customer_record>
        """;
    assertDoesNotThrow(() -> validator.validate(new StreamSource(new StringReader(outOfOrderXml))));
  }
}
