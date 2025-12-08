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

package org.apache.hop.mongo.wrapper.field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MongoArrayExpansionTest {

  private IVariables variables;
  private IRowMeta rowMeta;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    variables = new Variables();
    rowMeta = new RowMeta();
  }

  @Test
  void testInitWithEmptyPathThrowsException() {
    List<MongoField> subFields = new ArrayList<>();
    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "";
    expansion.outputRowMeta = rowMeta;

    assertThrows(HopException.class, expansion::init);
  }

  @Test
  void testInitWithNullPathThrowsException() {
    List<MongoField> subFields = new ArrayList<>();
    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = null;
    expansion.outputRowMeta = rowMeta;

    assertThrows(HopException.class, expansion::init);
  }

  @Test
  void testInitWithValidPath() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("field1"));

    MongoField subField = new MongoField();
    subField.fieldName = "field1";
    subField.fieldPath = "$.value";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.items[*]";
    expansion.outputRowMeta = rowMeta;

    expansion.init();
    // No exception means success
  }

  @Test
  void testInitWithRootPath() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("field1"));

    MongoField subField = new MongoField();
    subField.fieldName = "field1";
    subField.fieldPath = "$.value";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.data[*]";
    expansion.outputRowMeta = rowMeta;

    expansion.init();
    // $ root indicator should be removed
  }

  @Test
  void testConvertToHopValueWithNullDocument() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("field1"));

    MongoField subField = new MongoField();
    subField.fieldName = "field1";
    subField.fieldPath = "$.value";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.items[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    Object[][] result = expansion.convertToHopValue((Document) null, variables);

    assertNotNull(result);
    assertEquals(1, result.length);
  }

  @Test
  void testConvertToHopValueWithSimpleArray() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("name"));

    MongoField subField = new MongoField();
    subField.fieldName = "name";
    subField.fieldPath = "$.name";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.users[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    Document doc = new Document();
    List<Document> users =
        Arrays.asList(
            new Document("name", "Alice"),
            new Document("name", "Bob"),
            new Document("name", "Charlie"));
    doc.put("users", users);

    Object[][] result = expansion.convertToHopValue(doc, variables);

    assertNotNull(result);
    assertEquals(3, result.length);
    assertEquals("Alice", result[0][0]);
    assertEquals("Bob", result[1][0]);
    assertEquals("Charlie", result[2][0]);
  }

  @Test
  void testConvertToHopValueWithMissingField() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("field1"));

    MongoField subField = new MongoField();
    subField.fieldName = "field1";
    subField.fieldPath = "$.value";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.nonexistent[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    Document doc = new Document("other", "value");

    Object[][] result = expansion.convertToHopValue(doc, variables);

    assertNotNull(result);
    assertEquals(1, result.length); // Returns null result
  }

  @Test
  void testConvertToHopValueWithNestedDocument() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("city"));

    MongoField subField = new MongoField();
    subField.fieldName = "city";
    subField.fieldPath = "$.city";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.data.locations[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    Document innerDoc = new Document();
    List<Document> locations =
        Arrays.asList(new Document("city", "New York"), new Document("city", "London"));
    innerDoc.put("locations", locations);

    Document doc = new Document("data", innerDoc);

    Object[][] result = expansion.convertToHopValue(doc, variables);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("New York", result[0][0]);
    assertEquals("London", result[1][0]);
  }

  @Test
  void testConvertToHopValueWithEmptyArray() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("name"));

    MongoField subField = new MongoField();
    subField.fieldName = "name";
    subField.fieldPath = "$.name";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.items[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    Document doc = new Document("items", new ArrayList<>());

    Object[][] result = expansion.convertToHopValue(doc, variables);

    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  void testConvertToHopValueWithNullList() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("field1"));

    MongoField subField = new MongoField();
    subField.fieldName = "field1";
    subField.fieldPath = "$.value";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.items[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    Object[][] result = expansion.convertToHopValue((List<?>) null, variables);

    assertNotNull(result);
    assertEquals(1, result.length);
  }

  @Test
  void testConvertToHopValueWithSpecificIndex() throws HopException {
    rowMeta.addValueMeta(new ValueMetaString("name"));

    MongoField subField = new MongoField();
    subField.fieldName = "name";
    subField.fieldPath = "$.name";
    subField.hopType = "String";

    List<MongoField> subFields = new ArrayList<>();
    subFields.add(subField);

    MongoArrayExpansion expansion = new MongoArrayExpansion(subFields);
    expansion.expansionPath = "$.items[1].data[*]";
    expansion.outputRowMeta = rowMeta;
    expansion.init();
    expansion.reset(variables);

    List<Document> dataList =
        Arrays.asList(new Document("name", "First"), new Document("name", "Second"));
    Document item0 = new Document("data", Arrays.asList(new Document("name", "Other")));
    Document item1 = new Document("data", dataList);

    Document doc = new Document("items", Arrays.asList(item0, item1));

    Object[][] result = expansion.convertToHopValue(doc, variables);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("First", result[0][0]);
    assertEquals("Second", result[1][0]);
  }
}
