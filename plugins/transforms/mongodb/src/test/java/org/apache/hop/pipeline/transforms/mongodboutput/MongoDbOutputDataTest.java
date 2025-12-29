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

package org.apache.hop.pipeline.transforms.mongodboutput;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMeta.MongoIndex;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class MongoDbOutputDataTest {

  @Mock private IVariables variables;
  @Mock private MongoClientWrapper client;
  @Mock private MongoCollectionWrapper collection;
  @Mock private IRowMeta rowMeta;
  @Mock private IValueMeta valueMeta;

  @BeforeEach
  void before() {
    MockitoAnnotations.openMocks(this);
    when(variables.resolve(any(String.class)))
        .thenAnswer(
            (Answer<String>) invocationOnMock -> (String) invocationOnMock.getArguments()[0]);
    when(variables.resolve(any(String.class)))
        .thenAnswer(
            (Answer<String>) invocationOnMock -> (String) invocationOnMock.getArguments()[0]);
  }

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void testApplyIndexesOptions() throws HopException, MongoDbException {
    MongoDbOutputData data = new MongoDbOutputData();
    ILogChannel log = LogChannel.GENERAL;
    MongoCollectionWrapper collectionWrapper = mock(MongoCollectionWrapper.class);
    data.setCollection(collectionWrapper);

    ArgumentCaptor<Bson> captorIndexes = ArgumentCaptor.forClass(Bson.class);
    ArgumentCaptor<Bson> captorOptions = ArgumentCaptor.forClass(Bson.class);
    doNothing()
        .when(collectionWrapper)
        .createIndex(captorIndexes.capture(), captorOptions.capture());

    MongoIndex index = new MongoIndex();
    index.pathToFields = "FirstName:1";
    index.drop = false;
    index.sparse = false;
    index.unique = false;

    // Test with all options false
    data.applyIndexes(Collections.singletonList(index), log, false);
    Document createdIndex = (Document) captorIndexes.getValue();
    Document createdOptions = (Document) captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsKey("FirstName"));
    assertEquals(1, createdIndex.getInteger("FirstName").intValue());
    assertTrue(createdOptions.containsKey("background"));
    assertTrue(createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsKey("sparse"));
    assertFalse(createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsKey("unique"));
    assertFalse(createdOptions.getBoolean("unique"));

    // Test with only "sparse" true
    index.sparse = true;
    index.unique = false;
    data.applyIndexes(Collections.singletonList(index), log, false);
    createdIndex = (Document) captorIndexes.getValue();
    createdOptions = (Document) captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsKey("FirstName"));
    assertEquals(1, createdIndex.getInteger("FirstName").intValue());
    assertTrue(createdOptions.containsKey("background"));
    assertTrue(createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsKey("sparse"));
    assertTrue(createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsKey("unique"));
    assertFalse(createdOptions.getBoolean("unique"));

    // Test with only "unique" true
    index.sparse = false;
    index.unique = true;
    data.applyIndexes(Collections.singletonList(index), log, false);
    createdIndex = (Document) captorIndexes.getValue();
    createdOptions = (Document) captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsKey("FirstName"));
    assertEquals(1, createdIndex.getInteger("FirstName").intValue());
    assertTrue(createdOptions.containsKey("background"));
    assertTrue(createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsKey("sparse"));
    assertFalse(createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsKey("unique"));
    assertTrue(createdOptions.getBoolean("unique"));

    // Test with "sparse" and "unique" true
    index.sparse = true;
    index.unique = true;
    data.applyIndexes(Collections.singletonList(index), log, false);
    createdIndex = (Document) captorIndexes.getValue();
    createdOptions = (Document) captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsKey("FirstName"));
    assertEquals(1, createdIndex.getInteger("FirstName").intValue());
    assertTrue(createdOptions.containsKey("background"));
    assertTrue(createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsKey("sparse"));
    assertTrue(createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsKey("unique"));
    assertTrue(createdOptions.getBoolean("unique"));
  }

  @Test
  void testApplyIndexesSplits() throws HopException, MongoDbException {
    MongoDbOutputData data = new MongoDbOutputData();
    ILogChannel log = LogChannel.GENERAL;
    MongoCollectionWrapper collectionWrapper = mock(MongoCollectionWrapper.class);
    data.setCollection(collectionWrapper);

    ArgumentCaptor<Bson> captorIndexes = ArgumentCaptor.forClass(Bson.class);
    doNothing().when(collectionWrapper).createIndex(captorIndexes.capture(), any(Bson.class));

    MongoIndex index = new MongoIndex();
    index.pathToFields = "FirstName:1";
    index.drop = false;
    index.sparse = false;
    index.unique = false;

    data.applyIndexes(Collections.singletonList(index), log, false);
    Document createdIndex = (Document) captorIndexes.getValue();
    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsKey("FirstName"));
    assertEquals(1, createdIndex.getInteger("FirstName").intValue());

    // Test multiple fields
    index.pathToFields = "FirstName:1,LastName:-1,Street:1";
    data.applyIndexes(Collections.singletonList(index), log, false);
    createdIndex = (Document) captorIndexes.getValue();
    assertEquals(3, createdIndex.size());
    assertTrue(createdIndex.containsKey("FirstName"));
    assertEquals(1, createdIndex.getInteger("FirstName").intValue());
    assertTrue(createdIndex.containsKey("LastName"));
    assertEquals(-1, createdIndex.getInteger("LastName").intValue());
    assertTrue(createdIndex.containsKey("Street"));
    assertEquals(1, createdIndex.getInteger("Street").intValue());
  }

  @Test
  void testSetInitGet() throws HopException {
    // validates setting, initializing, and getting of MongoFields.
    MongoDbOutputMeta.MongoField field1 = new MongoDbOutputMeta.MongoField();
    MongoDbOutputMeta.MongoField field2 = new MongoDbOutputMeta.MongoField();
    field1.incomingFieldName = "field1";
    field1.mongoDocPath = "parent.field1";
    field2.incomingFieldName = "field2";
    field2.mongoDocPath = "parent.field2";

    MongoDbOutputData data = new MongoDbOutputData();
    data.setMongoFields(Arrays.asList(field1, field2));
    data.init(variables);

    List<MongoDbOutputMeta.MongoField> fields = data.getMongoFields();
    assertThat(fields.size(), equalTo(2));
    assertThat(fields.get(0).incomingFieldName, equalTo("field1"));
    assertThat(fields.get(1).incomingFieldName, equalTo("field2"));
    assertThat(fields.get(0).pathList, equalTo(Arrays.asList("parent", "field1")));
    assertThat(fields.get(1).pathList, equalTo(Arrays.asList("parent", "field2")));
  }

  @Test
  void testGetQueryObjectWithIncomingJson() throws HopException {
    MongoDbOutputMeta.MongoField field1 = new MongoDbOutputMeta.MongoField();
    field1.inputJson = true;
    field1.updateMatchField = true;
    when(rowMeta.getValueMeta(anyInt())).thenReturn(valueMeta);
    String query = "{ foo : 'bar' }";
    when(valueMeta.getString(any(Object.class))).thenReturn(query);
    Object[] row = new Object[] {"foo"};

    when(valueMeta.isString()).thenReturn(false);
    try {
      MongoDbOutputData.getQueryObject(
          Collections.singletonList(field1),
          rowMeta,
          row,
          variables,
          MongoDbOutputData.MongoTopLevel.RECORD);
      fail("expected an exception, can't construct query from non-string.");
    } catch (Exception e) {
      assertThat(e, instanceOf(HopException.class));
    }

    when(valueMeta.isString()).thenReturn(true);
    Document result =
        MongoDbOutputData.getQueryObject(
            Collections.singletonList(field1),
            rowMeta,
            row,
            variables,
            MongoDbOutputData.MongoTopLevel.RECORD);
    // Compare the contents rather than toString() representation
    Document expected = Document.parse(query);
    assertThat(result.get("foo"), equalTo(expected.get("foo")));
  }

  @Test
  void testWrapperMethods() {
    MongoDbOutputData data = new MongoDbOutputData();
    data.setConnection(client);
    assertThat(data.getConnection(), equalTo(client));
    data.setCollection(collection);
    assertThat(data.getCollection(), equalTo(collection));
    data.setOutputRowMeta(rowMeta);
    assertThat(data.getOutputRowMeta(), equalTo(rowMeta));
  }
}
