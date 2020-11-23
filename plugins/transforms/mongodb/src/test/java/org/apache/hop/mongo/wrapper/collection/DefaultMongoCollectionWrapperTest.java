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
package org.apache.hop.mongo.wrapper.collection;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultMongoCollectionWrapperTest {

  private DefaultMongoCollectionWrapper defaultMongoCollectionWrapper;
  @Mock private DBCollection mockDBCollection;
  @Mock private BasicDBObject dbObject;
  @Mock private List<DBObject> dbObjList;

  private DBObject[] dbObjectArray = new DBObject[0];

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    defaultMongoCollectionWrapper = new DefaultMongoCollectionWrapper(mockDBCollection);
  }

  @Test
  public void testRemove() throws Exception {
    defaultMongoCollectionWrapper.remove();
    verify(mockDBCollection, times(1)).remove(eq(new BasicDBObject()));
  }

  @Test
  public void testCreateIndex() throws Exception {
    BasicDBObject index = mock(BasicDBObject.class);
    BasicDBObject options = mock(BasicDBObject.class);
    defaultMongoCollectionWrapper.createIndex(index, options);
    verify(mockDBCollection).createIndex(index, options);
  }

  @Test
  public void testPassThroughMethods() throws MongoDbException {
    // Setup aggregate to use MongoDB Cursor method instead
    AggregationOptions options = AggregationOptions.builder().build();
    List<DBObject> pipeline = new ArrayList<>(); // can be empty

    defaultMongoCollectionWrapper.drop();
    verify(mockDBCollection).drop();
    defaultMongoCollectionWrapper.aggregate(pipeline, options);
    verify(mockDBCollection).aggregate(pipeline, options);
    defaultMongoCollectionWrapper.update(dbObject, dbObject, true, true);
    verify(mockDBCollection).update(dbObject, dbObject, true, true);
    defaultMongoCollectionWrapper.insert(dbObjList);
    verify(mockDBCollection).insert(dbObjList);
    defaultMongoCollectionWrapper.dropIndex(dbObject);
    verify(mockDBCollection).dropIndex(dbObject);
    defaultMongoCollectionWrapper.createIndex(dbObject);
    verify(mockDBCollection).createIndex(dbObject);
    defaultMongoCollectionWrapper.save(dbObject);
    verify(mockDBCollection).save(dbObject);
    defaultMongoCollectionWrapper.count();
    verify(mockDBCollection).count();
    defaultMongoCollectionWrapper.distinct("key");
    verify(mockDBCollection).distinct("key");
  }

  @Test
  public void testAggregate() throws MongoDbException {
    Cursor mockCursor = mock(Cursor.class);
    when(mockDBCollection.aggregate(anyList(), any(AggregationOptions.class)))
        .thenReturn(mockCursor);
    Cursor ret = defaultMongoCollectionWrapper.aggregate(dbObject, dbObjectArray);
    assertEquals(mockCursor, ret);
  }

  @Test
  public void testFindWrapsCursor() throws MongoDbException {
    assertThat(
        defaultMongoCollectionWrapper.find(), CoreMatchers.instanceOf(MongoCursorWrapper.class));
    verify(mockDBCollection).find();
    assertThat(
        defaultMongoCollectionWrapper.find(dbObject, dbObject),
        CoreMatchers.instanceOf(MongoCursorWrapper.class));
    verify(mockDBCollection).find(dbObject, dbObject);
    assertThat(
        defaultMongoCollectionWrapper.find(dbObject),
        CoreMatchers.instanceOf(MongoCursorWrapper.class));
    verify(mockDBCollection).find(dbObject);
  }
}
