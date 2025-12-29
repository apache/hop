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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DefaultMongoCollectionWrapperTest {

  private DefaultMongoCollectionWrapper defaultMongoCollectionWrapper;
  @Mock private MongoCollection<Document> mockMongoCollection;
  @Mock private Document document;
  @Mock private List<Document> docList;

  private Bson[] bsonArray = new Bson[0];

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    MockitoAnnotations.openMocks(this);
    defaultMongoCollectionWrapper = new DefaultMongoCollectionWrapper(mockMongoCollection);
  }

  @Test
  void testRemove() throws Exception {
    DeleteResult deleteResult = mock(DeleteResult.class);
    when(mockMongoCollection.deleteMany(any(Bson.class))).thenReturn(deleteResult);
    defaultMongoCollectionWrapper.remove();
    verify(mockMongoCollection, times(1)).deleteMany(any(Document.class));
  }

  @Test
  void testCreateIndex() throws Exception {
    Document index = mock(Document.class);
    Document options = new Document();
    options.put("background", true);
    options.put("unique", true);
    defaultMongoCollectionWrapper.createIndex(index, options);
    verify(mockMongoCollection).createIndex(any(Bson.class), any(IndexOptions.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPassThroughMethods() throws MongoDbException {
    // Setup aggregate to use MongoDB fluent API
    AggregateIterable<Document> mockAggregateIterable = mock(AggregateIterable.class);
    List<Bson> pipeline = new ArrayList<>();

    when(mockMongoCollection.aggregate(anyList())).thenReturn(mockAggregateIterable);
    when(mockMongoCollection.updateOne(any(Bson.class), any(Bson.class), any()))
        .thenReturn(mock(UpdateResult.class));
    when(mockMongoCollection.updateMany(any(Bson.class), any(Bson.class), any()))
        .thenReturn(mock(UpdateResult.class));
    when(mockMongoCollection.insertMany(anyList())).thenReturn(mock(InsertManyResult.class));

    // Mock distinct to return a proper iterable
    com.mongodb.client.DistinctIterable<Object> mockDistinctIterable =
        mock(com.mongodb.client.DistinctIterable.class);
    when(mockMongoCollection.distinct(any(String.class), any(Class.class)))
        .thenReturn(mockDistinctIterable);
    when(mockDistinctIterable.into(any())).thenReturn(new ArrayList<>());

    defaultMongoCollectionWrapper.drop();
    verify(mockMongoCollection).drop();

    defaultMongoCollectionWrapper.aggregate(pipeline);
    verify(mockMongoCollection).aggregate(anyList());

    defaultMongoCollectionWrapper.update(document, document, true, false);
    verify(mockMongoCollection).updateOne(any(Bson.class), any(Bson.class), any());

    defaultMongoCollectionWrapper.insert(docList);
    verify(mockMongoCollection).insertMany(anyList());

    defaultMongoCollectionWrapper.dropIndex(document);
    verify(mockMongoCollection).dropIndex(any(Bson.class));

    defaultMongoCollectionWrapper.createIndex(document);
    verify(mockMongoCollection).createIndex(any(Bson.class));

    defaultMongoCollectionWrapper.count();
    verify(mockMongoCollection).countDocuments();

    defaultMongoCollectionWrapper.distinct("key");
    verify(mockMongoCollection).distinct(any(String.class), any(Class.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAggregate() throws MongoDbException {
    AggregateIterable<Document> mockAggregateIterable = mock(AggregateIterable.class);
    when(mockMongoCollection.aggregate(anyList())).thenReturn(mockAggregateIterable);
    AggregateIterable<Document> ret = defaultMongoCollectionWrapper.aggregate(document, bsonArray);
    assertNotNull(ret);
    assertEquals(mockAggregateIterable, ret);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFindWrapsCursor() throws MongoDbException {
    FindIterable<Document> mockFindIterable = mock(FindIterable.class);
    when(mockMongoCollection.find()).thenReturn(mockFindIterable);
    when(mockMongoCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.projection(any(Bson.class))).thenReturn(mockFindIterable);

    assertTrue(defaultMongoCollectionWrapper.find() instanceof MongoCursorWrapper);
    verify(mockMongoCollection).find();

    assertTrue(
        defaultMongoCollectionWrapper.find(document, document) instanceof MongoCursorWrapper);
    verify(mockMongoCollection).find(any(Bson.class));

    assertTrue(defaultMongoCollectionWrapper.find(document) instanceof MongoCursorWrapper);
  }
}
