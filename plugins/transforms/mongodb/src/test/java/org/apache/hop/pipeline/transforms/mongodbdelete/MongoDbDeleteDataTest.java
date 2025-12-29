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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MongoDbDeleteDataTest {

  @Mock private MongoClientWrapper clientWrapper;
  @Mock private MongoCollectionWrapper collectionWrapper;
  @Mock private IRowMeta outputRowMeta;

  private MongoDbDeleteData deleteData;
  private IVariables variables;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    deleteData = new MongoDbDeleteData();
    variables = new Variables();
  }

  @Test
  void testSetAndGetConnection() {
    deleteData.setConnection(clientWrapper);
    assertEquals(clientWrapper, deleteData.getConnection());
  }

  @Test
  void testSetAndGetCollection() {
    deleteData.setCollection(collectionWrapper);
    assertEquals(collectionWrapper, deleteData.getCollection());
  }

  @Test
  void testSetAndGetOutputRowMeta() {
    deleteData.setOutputRowMeta(outputRowMeta);
    assertEquals(outputRowMeta, deleteData.getOutputRowMeta());
  }

  @Test
  void testCreateCollectionWithNullWrapper() {
    assertThrows(Exception.class, () -> deleteData.createCollection("db", "collection"));
  }

  @Test
  void testCreateCollection() throws Exception {
    deleteData.setConnection(clientWrapper);
    deleteData.createCollection("testDb", "testCollection");
    verify(clientWrapper).createCollection("testDb", "testCollection");
  }

  @Test
  void testSetMongoFields() throws HopException {
    List<MongoDbDeleteField> fields = new ArrayList<>();
    MongoDbDeleteField field1 = new MongoDbDeleteField();
    field1.mongoDocPath = "field1";
    fields.add(field1);

    deleteData.setMongoFields(fields);
    // Fields are copied, so modifying original list shouldn't affect internal state
    fields.clear();

    // init should work with the copied fields
    deleteData.init(variables);
  }

  @Test
  void testGetQueryObjectWithEqualComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "username";
    field.comparator = "=";
    field.incomingField1 = "name";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {"testUser"};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    assertEquals("testUser", query.get("username"));
  }

  @Test
  void testGetQueryObjectWithNotEqualComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("status"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "status";
    field.comparator = "<>";
    field.incomingField1 = "status";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {"inactive"};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document neDoc = (Document) query.get("status");
    assertEquals("inactive", neDoc.get("$ne"));
  }

  @Test
  void testGetQueryObjectWithGreaterThanComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("age"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "age";
    field.comparator = ">";
    field.incomingField1 = "age";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {18L};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document gtDoc = (Document) query.get("age");
    assertEquals(18L, gtDoc.get("$gt"));
  }

  @Test
  void testGetQueryObjectWithGreaterThanEqualComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaNumber("score"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "score";
    field.comparator = ">=";
    field.incomingField1 = "score";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {90.5};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document gteDoc = (Document) query.get("score");
    assertEquals(90.5, gteDoc.get("$gte"));
  }

  @Test
  void testGetQueryObjectWithLessThanComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("count"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "count";
    field.comparator = "<";
    field.incomingField1 = "count";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {100L};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document ltDoc = (Document) query.get("count");
    assertEquals(100L, ltDoc.get("$lt"));
  }

  @Test
  void testGetQueryObjectWithLessThanEqualComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaNumber("price"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "price";
    field.comparator = "<=";
    field.incomingField1 = "price";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {99.99};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document lteDoc = (Document) query.get("price");
    assertEquals(99.99, lteDoc.get("$lte"));
  }

  @Test
  void testGetQueryObjectWithBetweenComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("min"));
    rowMeta.addValueMeta(new ValueMetaInteger("max"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "value";
    field.comparator = "BETWEEN";
    field.incomingField1 = "min";
    field.incomingField2 = "max";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {10L, 100L};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document betweenDoc = (Document) query.get("value");
    assertEquals(10L, betweenDoc.get("$gt"));
    assertEquals(100L, betweenDoc.get("$lt"));
  }

  @Test
  void testGetQueryObjectWithIsNullComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "optional_field";
    field.comparator = "IS NULL";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document existDoc = (Document) query.get("optional_field");
    assertEquals(false, existDoc.get("$exists"));
  }

  @Test
  void testGetQueryObjectWithIsNotNullComparator() throws HopException {
    RowMeta rowMeta = new RowMeta();

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "required_field";
    field.comparator = "IS NOT NULL";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    Document existDoc = (Document) query.get("required_field");
    assertEquals(true, existDoc.get("$exists"));
  }

  @Test
  void testGetQueryObjectWithArrayPath() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tag"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "tags[0]";
    field.comparator = "=";
    field.incomingField1 = "tag";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {"important"};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    // Array notation should be converted to dot notation
    assertEquals("important", query.get("tags.0"));
  }

  @Test
  void testGetQueryObjectNoPathThrowsException() {
    RowMeta rowMeta = new RowMeta();

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = ""; // Empty path
    field.comparator = "=";
    field.incomingField1 = "name";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {"value"};

    assertThrows(
        HopException.class,
        () -> MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables));
  }

  @Test
  void testGetQueryObjectUnsupportedComparatorThrowsException() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "field";
    field.comparator = "UNSUPPORTED";
    field.incomingField1 = "field";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {"value"};

    assertThrows(
        HopException.class,
        () -> MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables));
  }

  @Test
  void testGetQueryObjectBetweenMissingFieldsThrowsException() {
    RowMeta rowMeta = new RowMeta();

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "value";
    field.comparator = "BETWEEN";
    field.incomingField1 = "min";
    field.incomingField2 = ""; // Missing second field

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {};

    assertThrows(
        HopException.class,
        () -> MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables));
  }

  @Test
  void testGetQueryObjectWithBooleanField() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBoolean("active"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "isActive";
    field.comparator = "=";
    field.incomingField1 = "active";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Object[] row = new Object[] {true};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    assertEquals(true, query.get("isActive"));
  }

  @Test
  void testGetQueryObjectWithDateField() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("created"));

    MongoDbDeleteField field = new MongoDbDeleteField();
    field.mongoDocPath = "createdAt";
    field.comparator = "=";
    field.incomingField1 = "created";

    List<MongoDbDeleteField> fields = new ArrayList<>();
    fields.add(field);

    Date testDate = new Date();
    Object[] row = new Object[] {testDate};

    Document query = MongoDbDeleteData.getQueryObject(fields, rowMeta, row, variables);

    assertNotNull(query);
    assertEquals(testDate, query.get("createdAt"));
  }

  @Test
  void testCleansePath() {
    // Test with variable containing dots
    assertEquals("path.${var_name}", MongoDbDeleteData.cleansePath("path.${var.name}"));

    // Test without variables
    assertEquals("simple.path", MongoDbDeleteData.cleansePath("simple.path"));

    // Test multiple variables
    assertEquals(
        "${var1_name}.${var2_name}", MongoDbDeleteData.cleansePath("${var1.name}.${var2.name}"));
  }

  @Test
  void testCleansePathNoVariables() {
    String path = "my.simple.path";
    assertEquals(path, MongoDbDeleteData.cleansePath(path));
  }
}
