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

package org.apache.hop.mongo.wrapper.cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.hop.mongo.MongoDbException;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DefaultCursorWrapperTest {

  @Mock private FindIterable<Document> findIterable;
  @Mock private MongoCursor<Document> mongoCursor;

  private DefaultCursorWrapper cursorWrapper;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(findIterable.iterator()).thenReturn(mongoCursor);
    cursorWrapper = new DefaultCursorWrapper(findIterable);
  }

  @Test
  void testHasNextTrue() throws MongoDbException {
    when(mongoCursor.hasNext()).thenReturn(true);
    assertTrue(cursorWrapper.hasNext());
    verify(findIterable).iterator();
  }

  @Test
  void testHasNextFalse() throws MongoDbException {
    when(mongoCursor.hasNext()).thenReturn(false);
    assertFalse(cursorWrapper.hasNext());
  }

  @Test
  void testNext() throws MongoDbException {
    Document expectedDoc = new Document("key", "value");
    ServerAddress serverAddress = new ServerAddress("localhost", 27017);

    when(mongoCursor.next()).thenReturn(expectedDoc);
    when(mongoCursor.getServerAddress()).thenReturn(serverAddress);

    Document result = cursorWrapper.next();

    assertEquals(expectedDoc, result);
    verify(mongoCursor).next();
  }

  @Test
  void testGetServerAddressAfterNext() throws MongoDbException {
    Document doc = new Document("key", "value");
    ServerAddress expectedAddress = new ServerAddress("localhost", 27017);

    when(mongoCursor.next()).thenReturn(doc);
    when(mongoCursor.getServerAddress()).thenReturn(expectedAddress);

    cursorWrapper.next(); // This should capture the server address
    ServerAddress result = cursorWrapper.getServerAddress();

    assertEquals(expectedAddress, result);
  }

  @Test
  void testGetServerAddressWithoutNext() throws MongoDbException {
    ServerAddress expectedAddress = new ServerAddress("localhost", 27017);
    when(mongoCursor.getServerAddress()).thenReturn(expectedAddress);

    ServerAddress result = cursorWrapper.getServerAddress();

    assertEquals(expectedAddress, result);
  }

  @Test
  void testClose() throws MongoDbException {
    // First call hasNext to initialize cursor
    when(mongoCursor.hasNext()).thenReturn(false);
    cursorWrapper.hasNext();

    cursorWrapper.close();

    verify(mongoCursor).close();
  }

  @Test
  void testCloseWithoutCursorInitialization() throws MongoDbException {
    // Close without ever using the cursor
    cursorWrapper.close();

    // Should not throw and cursor.close() should not be called since cursor was never initialized
    verify(mongoCursor, never()).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testLimit() throws MongoDbException {
    FindIterable<Document> limitedIterable = mock(FindIterable.class);
    when(findIterable.limit(10)).thenReturn(limitedIterable);

    MongoCursorWrapper result = cursorWrapper.limit(10);

    assertNotNull(result);
    assertTrue(result instanceof DefaultCursorWrapper);
    verify(findIterable).limit(10);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testLimitClosesExistingCursor() throws MongoDbException {
    // First initialize the cursor by calling hasNext
    when(mongoCursor.hasNext()).thenReturn(false);
    cursorWrapper.hasNext();

    FindIterable<Document> limitedIterable = mock(FindIterable.class);
    when(findIterable.limit(5)).thenReturn(limitedIterable);

    cursorWrapper.limit(5);

    // Existing cursor should be closed
    verify(mongoCursor).close();
  }

  @Test
  void testMultipleNextCalls() throws MongoDbException {
    Document doc1 = new Document("key", "value1");
    Document doc2 = new Document("key", "value2");
    ServerAddress serverAddress = new ServerAddress("localhost", 27017);

    when(mongoCursor.next()).thenReturn(doc1, doc2);
    when(mongoCursor.getServerAddress()).thenReturn(serverAddress);

    Document result1 = cursorWrapper.next();
    Document result2 = cursorWrapper.next();

    assertEquals(doc1, result1);
    assertEquals(doc2, result2);
  }

  @Test
  void testCursorIsLazilyInitialized() throws MongoDbException {
    // Initially, iterator() should not be called
    verify(findIterable, never()).iterator();

    // After calling hasNext, iterator() should be called
    when(mongoCursor.hasNext()).thenReturn(true);
    cursorWrapper.hasNext();

    verify(findIterable).iterator();
  }
}
