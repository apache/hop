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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMeta.MongoIndex;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MongoDbOutputDataTest {

  @Mock private IVariables variables;
  @Mock private MongoClientWrapper client;
  @Mock private MongoCollectionWrapper collection;
  @Mock private IRowMeta rowMeta;
  @Mock private IValueMeta valueMeta;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    when(variables.resolve(any(String.class)))
        .thenAnswer(
            (Answer<String>) invocationOnMock -> (String) invocationOnMock.getArguments()[0]);
    when(variables.resolve(any(String.class)))
        .thenAnswer(
            (Answer<String>) invocationOnMock -> (String) invocationOnMock.getArguments()[0]);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testApplyIndexesOptions() throws HopException, MongoDbException {
    MongoDbOutputData data = new MongoDbOutputData();
    ILogChannel log = LogChannel.GENERAL;
    DBCollection collection = mock(DBCollection.class);
    MongoCollectionWrapper collectionWrapper = spy(new DefaultMongoCollectionWrapper(collection));
    data.setCollection(collectionWrapper);

    ArgumentCaptor<BasicDBObject> captorIndexes = ArgumentCaptor.forClass(BasicDBObject.class);
    ArgumentCaptor<BasicDBObject> captorOptions = ArgumentCaptor.forClass(BasicDBObject.class);
    doNothing()
        .when(collectionWrapper)
        .createIndex(captorIndexes.capture(), captorOptions.capture());

    MongoIndex index = new MongoIndex();
    index.m_pathToFields = "FirstName:1";
    index.m_drop = false;
    index.m_sparse = false;
    index.m_unique = false;

    // Test with all options false
    data.applyIndexes(Arrays.asList(index), log, false);
    BasicDBObject createdIndex = captorIndexes.getValue();
    BasicDBObject createdOptions = captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsField("FirstName"));
    assertEquals("1", createdIndex.getString("FirstName"));
    assertTrue(createdOptions.containsField("background"));
    assertEquals(true, createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsField("sparse"));
    assertEquals(false, createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsField("unique"));
    assertEquals(false, createdOptions.getBoolean("unique"));

    // Test with only "sparse" true
    index.m_sparse = true;
    index.m_unique = false;
    data.applyIndexes(Arrays.asList(index), log, false);
    createdIndex = captorIndexes.getValue();
    createdOptions = captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsField("FirstName"));
    assertEquals("1", createdIndex.getString("FirstName"));
    assertTrue(createdOptions.containsField("background"));
    assertEquals(true, createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsField("sparse"));
    assertEquals(true, createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsField("unique"));
    assertEquals(false, createdOptions.getBoolean("unique"));

    // Test with only "unique" true
    index.m_sparse = false;
    index.m_unique = true;
    data.applyIndexes(Arrays.asList(index), log, false);
    createdIndex = captorIndexes.getValue();
    createdOptions = captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsField("FirstName"));
    assertEquals("1", createdIndex.getString("FirstName"));
    assertTrue(createdOptions.containsField("background"));
    assertEquals(true, createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsField("sparse"));
    assertEquals(false, createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsField("unique"));
    assertEquals(true, createdOptions.getBoolean("unique"));

    // Test with "sparse" and "unique" true
    index.m_sparse = true;
    index.m_unique = true;
    data.applyIndexes(Arrays.asList(index), log, false);
    createdIndex = captorIndexes.getValue();
    createdOptions = captorOptions.getValue();

    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsField("FirstName"));
    assertEquals("1", createdIndex.getString("FirstName"));
    assertTrue(createdOptions.containsField("background"));
    assertEquals(true, createdOptions.getBoolean("background"));
    assertTrue(createdOptions.containsField("sparse"));
    assertEquals(true, createdOptions.getBoolean("sparse"));
    assertTrue(createdOptions.containsField("unique"));
    assertEquals(true, createdOptions.getBoolean("unique"));
  }

  @Test
  public void testApplyIndexesSplits() throws HopException, MongoDbException {
    MongoDbOutputData data = new MongoDbOutputData();
    ILogChannel log = LogChannel.GENERAL;
    DBCollection collection = mock(DBCollection.class);
    MongoCollectionWrapper collectionWrapper = spy(new DefaultMongoCollectionWrapper(collection));
    data.setCollection(collectionWrapper);

    ArgumentCaptor<BasicDBObject> captorIndexes = ArgumentCaptor.forClass(BasicDBObject.class);
    doNothing()
        .when(collectionWrapper)
        .createIndex(captorIndexes.capture(), any(BasicDBObject.class));

    MongoIndex index = new MongoIndex();
    index.m_pathToFields = "FirstName:1";
    index.m_drop = false;
    index.m_sparse = false;
    index.m_unique = false;

    data.applyIndexes(Arrays.asList(index), log, false);
    BasicDBObject createdIndex = captorIndexes.getValue();
    assertEquals(1, createdIndex.size());
    assertTrue(createdIndex.containsField("FirstName"));
    assertEquals("1", createdIndex.getString("FirstName"));

    // Test multiple fields
    index.m_pathToFields = "FirstName:1,LastName:-1,Street:1";
    data.applyIndexes(Arrays.asList(index), log, false);
    createdIndex = captorIndexes.getValue();
    assertEquals(3, createdIndex.size());
    assertTrue(createdIndex.containsField("FirstName"));
    assertEquals("1", createdIndex.getString("FirstName"));
    assertTrue(createdIndex.containsField("LastName"));
    assertEquals("-1", createdIndex.getString("LastName"));
    assertTrue(createdIndex.containsField("Street"));
    assertEquals("1", createdIndex.getString("Street"));
  }

  @Test
  public void testSetInitGet() throws HopException {
    // validates setting, initializing, and getting of MongoFields.
    MongoDbOutputMeta.MongoField field1 = new MongoDbOutputMeta.MongoField();
    MongoDbOutputMeta.MongoField field2 = new MongoDbOutputMeta.MongoField();
    field1.m_incomingFieldName = "field1";
    field1.m_mongoDocPath = "parent.field1";
    field2.m_incomingFieldName = "field2";
    field2.m_mongoDocPath = "parent.field2";

    MongoDbOutputData data = new MongoDbOutputData();
    data.setMongoFields(Arrays.asList(field1, field2));
    data.init(variables);

    List<MongoDbOutputMeta.MongoField> fields = data.getMongoFields();
    assertThat(fields.size(), equalTo(2));
    assertThat(fields.get(0).m_incomingFieldName, equalTo("field1"));
    assertThat(fields.get(1).m_incomingFieldName, equalTo("field2"));
    assertThat(fields.get(0).m_pathList, equalTo(Arrays.asList("parent", "field1")));
    assertThat(fields.get(1).m_pathList, equalTo(Arrays.asList("parent", "field2")));
  }

  @Test
  public void testGetQueryObjectWithIncomingJson() throws HopException {
    MongoDbOutputMeta.MongoField field1 = new MongoDbOutputMeta.MongoField();
    field1.m_JSON = true;
    field1.m_updateMatchField = true;
    when(rowMeta.getValueMeta(anyInt())).thenReturn(valueMeta);
    String query = "{ foo : 'bar' }";
    when(valueMeta.getString(any(Object[].class))).thenReturn(query);
    Object[] row = new Object[] {"foo"};

    when(valueMeta.isString()).thenReturn(false);
    try {
      MongoDbOutputData.getQueryObject(
          Arrays.asList(field1), rowMeta, row, variables, MongoDbOutputData.MongoTopLevel.RECORD);
      fail("expected an exception, can't construct query from non-string.");
    } catch (Exception e) {
      assertThat(e, instanceOf(HopException.class));
    }

    when(valueMeta.isString()).thenReturn(true);
    assertThat(
        MongoDbOutputData.getQueryObject(
            Arrays.asList(field1), rowMeta, row, variables, MongoDbOutputData.MongoTopLevel.RECORD),
        equalTo((DBObject) JSON.parse(query)));
  }

  @Test
  public void testWrapperMethods() {
    MongoDbOutputData data = new MongoDbOutputData();
    data.setConnection(client);
    assertThat(data.getConnection(), equalTo(client));
    data.setCollection(collection);
    assertThat(data.getCollection(), equalTo(collection));
    data.setOutputRowMeta(rowMeta);
    assertThat(data.getOutputRowMeta(), equalTo(rowMeta));
  }
}
