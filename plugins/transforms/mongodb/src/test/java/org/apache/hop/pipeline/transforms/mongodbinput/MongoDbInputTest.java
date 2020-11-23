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

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDbInputTest extends BaseMongoDbTransformTest {
  @Mock private MongoDbInputData iTransformData;
  @Mock private MongoDbInputMeta iTransformMeta;
  @Mock private MongoCursorWrapper mockCursor;

  private Object[] putRow;
  private MongoDbInput dbInput;

  @Before
  public void before() throws MongoDbException {
    super.before();

    when(iTransformMeta.getPort()).thenReturn("1010");
    when(iTransformMeta.getHostnames()).thenReturn("host");

    dbInput =
        new MongoDbInput(transformMeta, iTransformMeta, iTransformData, 1, pipelineMeta, pipeline) {
          public Object[] getRow() {
            return rowData;
          }

          public IRowMeta getInputRowMeta() {
            return rowMeta;
          }

          public void putRow(IRowMeta rowMeta, Object[] row) {
            putRow = row;
          }
        };
  }

  @Test
  public void testInitNoDbSpecified() {
    assertFalse(dbInput.init());
    verify(mockLog).logError(anyString(), throwableCaptor.capture());
    assertThat(throwableCaptor.getValue().getMessage(), containsString("No database specified"));
  }

  @Test
  public void testInitNoCollection() {
    when(iTransformMeta.getDbName()).thenReturn("dbname");
    assertFalse(dbInput.init());
    verify(mockLog).logError(anyString(), throwableCaptor.capture());
    assertThat(throwableCaptor.getValue().getMessage(), containsString("No collection specified"));
  }

  @Test
  public void testInit() throws MongoDbException {
    setupReturns();
    assertTrue(dbInput.init());
    assertThat(iTransformData.clientWrapper, equalTo(mongoClientWrapper));
    assertThat(iTransformData.collection, equalTo(mongoCollectionWrapper));
  }

  private void setupReturns() throws MongoDbException {
    when(iTransformMeta.getDbName()).thenReturn("dbname");
    when(iTransformMeta.getCollection()).thenReturn("collection");
    when(mongoClientWrapper.getCollection("dbname", "collection"))
        .thenReturn(mongoCollectionWrapper);
    when(mongoClientWrapper.getCollection("dbname", "collection"))
        .thenReturn(mongoCollectionWrapper);
    when(mongoCollectionWrapper.find()).thenReturn(mockCursor);
    when(mongoCollectionWrapper.find(any(DBObject.class), any(DBObject.class)))
        .thenReturn(mockCursor);
  }

  @Test
  public void processRowSinglePartAggPipelineQuery() throws HopException, MongoDbException {
    processRowWithQuery("{$match : { foo : 'bar'}}");
  }

  @Test
  public void processRowMultipartAggPipelineQuery() throws MongoDbException, HopException {
    processRowWithQuery("{$match : { foo : 'bar'}}, { $sort : 1 }");
  }

  private void processRowWithQuery(String query) throws MongoDbException, HopException {
    setupReturns();
    when(iTransformMeta.getJsonQuery()).thenReturn(query);
    when(iTransformMeta.isQueryIsPipeline()).thenReturn(true);
    String[] parts = query.split(",");
    DBObject dbObjQuery = (DBObject) JSON.parse(parts[0]);
    DBObject[] remainder =
        parts.length > 1 ? new DBObject[] {(DBObject) JSON.parse(parts[1])} : new DBObject[0];
    when(mongoCollectionWrapper.aggregate(dbObjQuery, remainder)).thenReturn(cursor);

    dbInput.init();
    dbInput.processRow();

    verify(iTransformData).init();
    verify(mongoCollectionWrapper).aggregate(dbObjQuery, remainder);
    assertEquals(cursor, iTransformData.m_pipelineResult);
  }

  @Test
  public void testSimpleFind() throws HopException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    dbInput.init();
    assertFalse("should return false as there are no more results", dbInput.processRow());
    verify(mongoCollectionWrapper).find();
    assertThat(iTransformData.cursor, equalTo(mockCursor));
  }

  @Test
  public void testDispose() throws HopException, MongoDbException {
    setupReturns();
    dbInput.init();
    dbInput.processRow();
    dbInput.dispose();
    verify(mockCursor).close();
    verify(mongoClientWrapper).dispose();

    MongoDbException mockException = mock(MongoDbException.class);
    when(mockException.getMessage()).thenReturn("error msg");
    doThrow(mockException).when(mockCursor).close();
    doThrow(mockException).when(mongoClientWrapper).dispose();
    dbInput.dispose();
    // error should be logged after curor.close failure and client.dispose failure.
    verify(mockLog, times(2)).logError("error msg");
  }

  @Test
  public void testFindWithMoreResults() throws HopException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    when(mockCursor.hasNext()).thenReturn(true);
    ServerAddress serverAddress = mock(ServerAddress.class);
    when(serverAddress.toString()).thenReturn("serveraddress");
    when(mockCursor.getServerAddress()).thenReturn(serverAddress);
    DBObject nextDoc = (DBObject) JSON.parse("{ 'foo' : 'bar' }");
    when(mockCursor.next()).thenReturn(nextDoc);
    dbInput.setStopped(false);
    dbInput.init();

    assertTrue("more results -> should return true", dbInput.processRow());
    verify(mongoCollectionWrapper).find();
    verify(mockCursor).next();
    verify(mockLog).logBasic(stringCaptor.capture());
    assertThat(stringCaptor.getValue(), containsString("serveraddress"));
    assertThat(iTransformData.cursor, equalTo(mockCursor));
    assertThat(putRow[0], CoreMatchers.<Object>equalTo(JSON.serialize(nextDoc)));
  }

  @Test
  public void testFindWithQuery() throws HopException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    String query = "{ type : 'heavy'} ";
    when(iTransformMeta.getJsonQuery()).thenReturn(query);
    dbInput.init();
    assertFalse("should return false as there are no more results", dbInput.processRow());
    verify(mongoCollectionWrapper).find(dbObjectCaptor.capture(), any(DBObject.class));
    assertThat(iTransformData.cursor, equalTo(mockCursor));
    assertThat(dbObjectCaptor.getValue(), equalTo((DBObject) JSON.parse(query)));
  }

  @Test
  public void testAuthUserLogged() throws MongoDbException {
    setupReturns();
    when(iTransformMeta.getAuthenticationUser()).thenReturn("joe_user");

    dbInput.init();
    verify(mockLog).logBasic(stringCaptor.capture());
    assertThat(stringCaptor.getValue(), containsString("joe_user"));
  }

  @Test
  public void testExecuteForEachIncomingRow() throws MongoDbException, HopException {
    setupReturns();
    when(iTransformMeta.getExecuteForEachIncomingRow()).thenReturn(true);
    when(iTransformMeta.getJsonQuery()).thenReturn("{ foo : ?{param}} ");
    rowData = new Object[] {"'bar'"};
    rowMeta.addValueMeta(new ValueMetaString("param"));
    dbInput.init();
    assertTrue(dbInput.processRow());
    verify(mongoCollectionWrapper).find(dbObjectCaptor.capture(), any(DBObject.class));
    assertThat(dbObjectCaptor.getValue(), equalTo((DBObject) JSON.parse("{foo : 'bar'}")));
  }
}
