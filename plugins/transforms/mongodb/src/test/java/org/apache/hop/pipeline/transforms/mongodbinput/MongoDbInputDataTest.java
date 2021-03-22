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

package org.apache.hop.pipeline.transforms.mongodbinput;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDbInputDataTest {
  private IHopMetadataProvider metadataProvider;
  private MongoDbInputData mongoDbInputData;

  protected static String s_testData =
      "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
          + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
          + "\"name\" : \"george\", \"aNumber\" : 42 }";
  protected static String s_testData2 =
      "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
          + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } }, "
          + "{ \"rec1\" : { \"f1\" : \"sid\", \"f2\" : \"zaphod\" } } ] }, "
          + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";

  @Before
  public void setUp() throws HopException {
    HopClientEnvironment.init();
    metadataProvider = mock(IHopMetadataProvider.class);
    mongoDbInputData = new MongoDbInputData();
  }



  @Test
  public void testDiscoverFieldsThrowsException() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock(MongoDbInputMeta.class);
    when(meta.getName()).thenReturn(dbName);
    when(meta.getCollection()).thenReturn(collection);
    when(meta.getJsonQuery()).thenReturn(query);
    when(meta.getFieldsName()).thenReturn(fields);

    IVariables vars = mock(IVariables.class);
    when(vars.resolve(dbName)).thenReturn(dbName);
    when(vars.resolve(collection)).thenReturn(collection);
    when(vars.resolve(query)).thenReturn(query);
    when(vars.resolve(fields)).thenReturn(fields);

    int docsToSample = 1;

    // Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock(MongoDbInputDiscoverFields.class);

    doThrow(new HopException())
        .when(mongoDbInputDiscoverFields)
        .discoverFields(
            any(IVariables.class),
            any(MongoDbConnection.class),
            anyString(),
            anyString(),
            anyString(),
            anyBoolean(),
            anyInt(),
            any(MongoDbInputMeta.class),
            any(DiscoverFieldsCallback.class));

    try {
      MongoDbInputDialog.discoverFields(meta, vars, docsToSample, metadataProvider);
    } catch (Exception expected) {
      // expected
    }
  }

  @Test
  public void testDiscoverFieldsWithoutCallbackThrowsHopException() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock(MongoDbInputMeta.class);
    when(meta.getName()).thenReturn(dbName);
    when(meta.getCollection()).thenReturn(collection);
    when(meta.getJsonQuery()).thenReturn(query);
    when(meta.getFieldsName()).thenReturn(fields);

    IVariables vars = mock(IVariables.class);
    when(vars.resolve(dbName)).thenReturn(dbName);
    when(vars.resolve(collection)).thenReturn(collection);
    when(vars.resolve(query)).thenReturn(query);
    when(vars.resolve(fields)).thenReturn(fields);

    int docsToSample = 1;

    // Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock(MongoDbInputDiscoverFields.class);
    when(mongoDbInputDiscoverFields.discoverFields(
            any(IVariables.class),
            any(MongoDbConnection.class),
            anyString(),
            anyString(),
            anyString(),
            anyBoolean(),
            anyInt(),
            any(MongoDbInputMeta.class)))
        .thenThrow(new HopException("testException"));

    try {
      MongoDbInputDialog.discoverFields(meta, vars, docsToSample, metadataProvider);
    } catch (HopException e) {
      // Expected
    }
  }

  @Test
  public void testDiscoverFieldsWithoutCallbackThrowsException() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock(MongoDbInputMeta.class);
    when(meta.getName()).thenReturn(dbName);
    when(meta.getCollection()).thenReturn(collection);
    when(meta.getJsonQuery()).thenReturn(query);
    when(meta.getFieldsName()).thenReturn(fields);

    IVariables vars = mock(IVariables.class);
    when(vars.resolve(dbName)).thenReturn(dbName);
    when(vars.resolve(collection)).thenReturn(collection);
    when(vars.resolve(query)).thenReturn(query);
    when(vars.resolve(fields)).thenReturn(fields);

    int docsToSample = 1;

    // Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock(MongoDbInputDiscoverFields.class);
    when(mongoDbInputDiscoverFields.discoverFields(
            any(IVariables.class),
            any(MongoDbConnection.class),
            anyString(),
            anyString(),
            anyString(),
            anyBoolean(),
            anyInt(),
            any(MongoDbInputMeta.class)))
        .thenThrow(new NullPointerException());

    try {
      MongoDbInputDialog.discoverFields(meta, vars, docsToSample, metadataProvider);
    } catch (HopException e) {
      // Expected
    }
  }

  @Test
  public void testGetNonExistentField() throws HopException {
    Object mongoO = BasicDBObject.parse(s_testData);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> discoveredFields = new ArrayList<>();
    MongoField mm = new MongoField();
    mm.fieldName = "test";
    mm.fieldPath = "$.iDontExist";
    mm.hopType = "String";
    discoveredFields.add(mm);

    IRowMeta rowMeta = new RowMeta();
    for (MongoField m : discoveredFields) {
      int type = ValueMetaFactory.getIdForValueMeta(m.hopType);
      IValueMeta vm = ValueMetaFactory.createValueMeta(m.fieldName, type);
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(discoveredFields);
    data.init();
    Variables vars = new Variables();
    Object[] result = data.mongoDocumentToHop((DBObject) mongoO, vars)[0];

    assertNotNull(result);
    assertEquals(1, result.length - RowDataUtil.OVER_ALLOCATE_SIZE);
    assertNull(result[0]);
  }

  @Test
  public void testArrayUnwindArrayFieldsOnly() throws HopException {
    Object mongoO = BasicDBObject.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> fields = new ArrayList<>();

    MongoField mm = new MongoField();
    mm.fieldName = "test";
    mm.fieldPath = "$.one.two[*].rec1.f1";
    mm.hopType = "String";

    fields.add(mm);
    IRowMeta rowMeta = new RowMeta();
    for (MongoField m : fields) {
      int type = ValueMetaFactory.getIdForValueMeta(m.hopType);
      IValueMeta vm = ValueMetaFactory.createValueMeta(m.fieldName, type);
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToHop((DBObject) mongoO, vars);

    assertNotNull(result);
    assertEquals(2, result.length);

    // should be two rows returned due to the array expansion
    assertNotNull(result[0]);
    assertNotNull(result[1]);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);
  }

  @Test
  public void testArrayUnwindOneArrayExpandFieldAndOneNormalField() throws HopException {
    Object mongoO = BasicDBObject.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> fields = new ArrayList<>();

    MongoField mm = new MongoField();
    mm.fieldName = "test";
    mm.fieldPath = "$.one.two[*].rec1.f1";
    mm.hopType = "String";
    fields.add(mm);

    mm = new MongoField();
    mm.fieldName = "test2";
    mm.fieldPath = "$.name";
    mm.hopType = "String";
    fields.add(mm);

    IRowMeta rowMeta = new RowMeta();
    for (MongoField m : fields) {
      int type = ValueMetaFactory.getIdForValueMeta(m.hopType);
      IValueMeta vm = ValueMetaFactory.createValueMeta(m.fieldName, type);
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToHop((DBObject) mongoO, vars);

    assertNotNull(result);
    assertEquals(2, result.length);

    // each row should have two entries
    assertEquals(2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length);

    // should be two rows returned due to the array expansion
    assertNotNull(result[0]);
    assertNotNull(result[1]);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);

    // george should be the name in both rows
    assertEquals("george", result[0][1]);
    assertEquals("george", result[1][1]);
  }

  @Test
  public void testArrayUnwindWithOneExistingAndOneNonExistingField() throws HopException {
    Object mongoO = BasicDBObject.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> fields = new ArrayList<>();

    MongoField mm = new MongoField();
    mm.fieldName = "test";
    mm.fieldPath = "$.one.two[*].rec1.f1";
    mm.hopType = "String";
    fields.add(mm);

    mm = new MongoField();
    mm.fieldName = "test2";
    mm.fieldPath = "$.one.two[*].rec6.nonExistent";
    mm.hopType = "String";
    fields.add(mm);

    IRowMeta rowMeta = new RowMeta();
    for (MongoField m : fields) {
      int type = ValueMetaFactory.getIdForValueMeta(m.hopType);
      IValueMeta vm = ValueMetaFactory.createValueMeta(m.fieldName, type);
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToHop((DBObject) mongoO, vars);

    assertNotNull(result);
    assertEquals(2, result.length);

    // should be two rows returned due to the array expansion
    assertNotNull(result[0]);
    assertNotNull(result[1]);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);

    // each row should have two entries
    assertEquals(2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length);

    // this field doesn't exist in the doc structure, so we expect null
    assertNull(result[0][1]);
    assertNull(result[1][1]);
  }

  @Test
  public void testCleansePath() {
    // param at end of path
    assertEquals(
        MongoDbInputData.cleansePath("my.path.with.${a.dot.param}"), "my.path.with.${a_dot_param}");
    // param at start of path
    assertEquals(
        MongoDbInputData.cleansePath("${a.dot.param}.my.path.with"), "${a_dot_param}.my.path.with");
    // param in middle of path
    assertEquals(
        MongoDbInputData.cleansePath("my.path.with.${a.dot.param}.otherstuff"),
        "my.path.with.${a_dot_param}.otherstuff");
    // multiple params
    assertEquals(
        MongoDbInputData.cleansePath("my.${oneparam}.with.${a.dot.param}.otherstuff"),
        "my.${oneparam}.with.${a_dot_param}.otherstuff");
  }
}
