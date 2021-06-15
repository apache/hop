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

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.MongoDBAction;
import org.apache.hop.mongo.wrapper.MongoWrapperClientFactory;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputMeta;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongodbInputDiscoverFieldsImplTest {

  private IVariables variables;
  @Mock private MongoDbConnection connection;
  @Mock private MongoWrapperClientFactory clientFactory;
  @Mock private MongoClientWrapper clientWrapper;
  @Mock private DB mockDb;
  @Mock private MongoDbInputMeta inputMeta;
  @Mock private DBCollection collection;
  @Mock private DBCursor cursor;
  @Captor private ArgumentCaptor<MongoProperties.Builder> propCaptor;
  @Captor private ArgumentCaptor<DBObject> dbObjectCaptor;
  @Captor private ArgumentCaptor<DBObject[]> dbObjectArrayCaptor;

  private MongodbInputDiscoverFieldsImpl discoverFields;
  private final int NUM_DOCS_TO_SAMPLE = 2;

  @Before
  public void before() throws MongoDbException, HopPluginException {
    variables = new Variables();
    MockitoAnnotations.initMocks(this);
    when(clientFactory.createMongoClientWrapper(
            any(MongoProperties.class), any(MongoUtilLogger.class)))
        .thenReturn(clientWrapper);
    when(mockDb.getCollection(any(String.class))).thenReturn(collection);
    when(collection.find()).thenReturn(cursor);
    when(cursor.limit(anyInt())).thenReturn(cursor);
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.init();
    discoverFields = mock(MongodbInputDiscoverFieldsImpl.class);
  }



  private void setupPerform() throws MongoDbException {
    when(clientWrapper.perform(any(String.class), any(MongoDBAction.class)))
        .thenAnswer(
            new Answer<List<MongoField>>() {
              @Override
              public List<MongoField> answer(InvocationOnMock invocationOnMock) throws Throwable {
                MongoDBAction action = (MongoDBAction) invocationOnMock.getArguments()[1];
                if (action != null) {
                  return (List<MongoField>) action.perform(mockDb);
                } else {
                  return null;
                }
              }
            });
    when(connection.createWrapper(any(), any())).thenReturn(mock(MongoClientWrapper.class));
    setupCursorWithNRows(NUM_DOCS_TO_SAMPLE);
  }

  private void setupCursorWithNRows(final int N) {
    when(cursor.hasNext())
        .thenAnswer(
            new Answer<Boolean>() {
              int count = 0;

              @Override
              public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                return count++ < N;
              }
            });
  }

  /**
   * Checks that each field has the expected trio of name, path, and hop value contained in the
   * expecteds vararg. The expecteds should contain an array of { nameForField1, pathForField1,
   * valueForField1, nameForField2, pathForField2, valueForField2, ... }
   */
  private void validateFields(List<MongoField> fields, Object... expecteds) throws Exception {
    setupPerform();
    assertThat(expecteds.length, equalTo(fields.size() * 3));
    Collections.sort(fields);
    for (int i = 0; i < fields.size(); i++) {
      fields.get(i).init(i);
      assertThat(fields.get(i).getName(), equalTo(expecteds[i * 3]));
      assertThat(fields.get(i).getPath(), equalTo(expecteds[i * 3 + 1]));
      assertThat(fields.get(i).getHopValue(expecteds[i * 3 + 2]), equalTo(expecteds[i * 3 + 2]));
    }
  }

  @Test
  public void testSetMinArrayIndexesNoArraysPresent() {
    MongoField m = new MongoField();
    m.fieldName = "bob.fred.george";
    m.fieldPath = "bob.fred.george";

    MongodbInputDiscoverFieldsImpl.setMinArrayIndexes(m);
    assertThat("bob.fred.george", equalTo(m.fieldName));
    assertThat("bob.fred.george", equalTo(m.fieldPath));
  }

  @Test
  public void testSetMinArrayIndexesOneArray() {
    MongoField m = new MongoField();
    m.fieldName = "bob.fred[2:10].george";
    m.fieldPath = "bob.fred[-].george";

    MongodbInputDiscoverFieldsImpl.setMinArrayIndexes(m);
    assertThat("bob.fred[2].george", equalTo(m.fieldPath));
  }

  @Test
  public void testSetMinArrayIndexesTwoArrays() {
    MongoField m = new MongoField();
    m.fieldName = "bob[5:5].fred[2:10].george";
    m.fieldPath = "bob[-].fred[-].george";

    MongodbInputDiscoverFieldsImpl.setMinArrayIndexes(m);
    assertThat("bob[5].fred[2].george", equalTo(m.fieldPath));
  }

  @Test
  public void testUpdateMinMaxArrayIndexes() {

    MongoField m = new MongoField();
    m.fieldName = "bob.fred[2:4].george";
    m.fieldPath = "bob.fred[-].george";

    MongodbInputDiscoverFieldsImpl.updateMinMaxArrayIndexes(m, "bob.fred[1:1].george");

    assertThat("bob.fred[1:4].george", equalTo(m.fieldName));
    MongodbInputDiscoverFieldsImpl.updateMinMaxArrayIndexes(m, "bob.fred[5:5].george");
    assertThat("bob.fred[1:5].george", equalTo(m.fieldName));
  }

  @Test
  public void testPostProcessPaths() {
    Map<String, MongoField> fieldMap = new LinkedHashMap<>();
    List<MongoField> discovered = new ArrayList<>();

    MongoField m = new MongoField();
    m.fieldPath = "bob.fred[-].george";
    m.fieldName = "bob.fred[2:10].george";
    m.percentageOfSample = 5;
    fieldMap.put(m.fieldPath, m);
    m = new MongoField();
    m.fieldPath = "one.two[-]";
    m.fieldName = "one.two[1]";
    m.percentageOfSample = 10;
    fieldMap.put(m.fieldPath, m);

    MongodbInputDiscoverFieldsImpl.postProcessPaths(fieldMap, discovered, 100);

    assertThat(2, equalTo(discovered.size()));
    m = discovered.get(0);
    assertThat("george", equalTo(m.fieldName));
    assertThat("bob.fred[2].george", equalTo(m.fieldPath));
    assertThat("5/100", equalTo(m.occurrenceFraction));
    assertThat("bob.fred[2:10].george", equalTo(m.arrayIndexInfo));

    m = discovered.get(1);
    assertThat("two[1]", equalTo(m.fieldName));
    assertThat("one.two[1]", equalTo(m.fieldPath));
    assertThat("10/100", equalTo(m.occurrenceFraction));
    assertThat(null, equalTo(m.arrayIndexInfo));
  }

  @Test
  public void testDocToFields() {
    Map<String, MongoField> fieldMap = new LinkedHashMap<>();
    DBObject doc =
        (DBObject) BasicDBObject.parse("{\"fred\" : {\"george\" : 1}, \"bob\" : [1 , 2]}");

    MongodbInputDiscoverFieldsImpl.docToFields(doc, fieldMap);
    assertThat(3, equalTo(fieldMap.size()));

    assertThat(fieldMap.get("$.fred.george"), notNullValue());
    assertThat(fieldMap.get("$.bob[0]"), notNullValue());
    assertThat(fieldMap.get("$.bob[1]"), notNullValue());
    assertThat(fieldMap.get("$.bob[2]"), equalTo(null));
  }
}
