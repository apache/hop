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

package org.apache.hop.mongo.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class MongoClientWrapperImplTest {

  public static final String REP_SET_CONFIG =
      "{\"_id\" : \"foo\", \"version\" : 1, "
          + "\"members\" : ["
          + "{"
          + "\"_id\" : 0, "
          + "\"host\" : \"palladium.lan:27017\", "
          + "\"tags\" : {"
          + "\"dc.one\" : \"primary\", "
          + "\"use\" : \"production\""
          + "}"
          + "}, "
          + "{"
          + "\"_id\" : 1, "
          + "\"host\" : \"palladium.local:27018\", "
          + "\"tags\" : {"
          + "\"dc.two\" : \"slave1\""
          + "}"
          + "}, "
          + "{"
          + "\"_id\" : 2, "
          + "\"host\" : \"palladium.local:27019\", "
          + "\"tags\" : {"
          + "\"dc.three\" : \"slave2\", "
          + "\"use\" : \"production\""
          + "}"
          + "}"
          + "],"
          + "\"settings\" : {"
          + "\"getLastErrorModes\" : { "
          + "\"DCThree\" : {"
          + "\"dc.three\" : 1"
          + "}"
          + "}"
          + "}"
          + "}";

  private static final String TAG_SET = "{\"use\" : \"production\"}";
  private static final String TAG_SET2 = "{\"use\" : \"ops\"}";

  @Mock private MongoClient mockMongoClient;
  @Mock private MongoUtilLogger mockMongoUtilLogger;
  @Mock private MongoProperties mongoProperties;
  @Mock private DefaultMongoClientFactory mongoClientFactory;
  @Mock private MongoDatabase mockDB;
  @Mock private MongoCollection<Document> collection;
  @Mock private RuntimeException runtimeException;
  @Captor private ArgumentCaptor<List<ServerAddress>> serverAddresses;
  @Captor private ArgumentCaptor<List<MongoCredential>> mongoCredentials;

  private MongoClientWrapperImpl mongoClientWrapper;

  private static final Class<?> PKG = MongoClientWrapperImpl.class;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(
            mongoClientFactory.getMongoClient(
                Mockito.anyList(),
                Mockito.anyList(),
                Mockito.any(MongoClientSettings.Builder.class),
                Mockito.anyBoolean()))
        .thenReturn(mockMongoClient);
    MongoClientWrapperImpl.clientFactory = mongoClientFactory;
    mongoClientWrapper =
        new MongoClientWrapperImpl(mockMongoClient, mongoProperties, mockMongoUtilLogger);
  }

  @Test
  void testPerform() throws Exception {
    Mockito.when(mockMongoClient.getDatabase("Test")).thenReturn(mockDB);
    MongoDBAction mockMongoDBAction = Mockito.mock(MongoDBAction.class);
    mongoClientWrapper.perform("Test", mockMongoDBAction);
    Mockito.verify(mockMongoDBAction, Mockito.times(1)).perform(mockDB);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetLastErrorMode() throws MongoDbException {
    Document config = Document.parse(REP_SET_CONFIG);
    MongoCollection<Document> replSetCollection = Mockito.mock(MongoCollection.class);
    FindIterable<Document> findIterable = Mockito.mock(FindIterable.class);

    Mockito.when(mockMongoClient.getDatabase(MongoClientWrapperImpl.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(MongoClientWrapperImpl.REPL_SET_COLLECTION))
        .thenReturn(replSetCollection);
    Mockito.when(replSetCollection.find()).thenReturn(findIterable);
    Mockito.when(findIterable.first()).thenReturn(config);

    assertEquals(Arrays.asList("DCThree"), mongoClientWrapper.getLastErrorModes());
  }

  @Test
  void testGetAllReplicaSetMemberRecords() {
    Document config = Document.parse(REP_SET_CONFIG);
    Object members = config.get(MongoClientWrapperImpl.REPL_SET_MEMBERS);

    assertNotNull(members);
    assertTrue(members instanceof List);
    assertEquals(3, ((List<?>) members).size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSetupAllTags() {
    Document config = Document.parse(REP_SET_CONFIG);
    List<Document> members = (List<Document>) config.get(MongoClientWrapperImpl.REPL_SET_MEMBERS);

    List<String> allTags = mongoClientWrapper.setupAllTags(members);

    assertEquals(4, allTags.size());
  }

  @Test
  void testGetReplicaSetMembersThatSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();

    List<Document> tagSets = new ArrayList<>(); // tags to satisfy

    Document tSet = Document.parse(TAG_SET);
    tagSets.add(tSet);

    List<String> satisfy = mongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
    // two replica set members have the "use : production" tag in their tag sets
    assertEquals(2, satisfy.size());
    assertTrue(satisfy.get(0).contains("palladium.lan:27017"));
    assertTrue(satisfy.get(1).contains("palladium.local:27019"));
  }

  @Test
  void testGetReplicaSetMembersBadInput() {
    setupMockedReplSet();
    try {
      mongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(null);
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof MongoDbException);
    }
  }

  @Test
  void testGetReplicaSetMembersDoesntSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();
    List<Document> tagSets = new ArrayList<>(); // tags to satisfy
    Document tSet = Document.parse(TAG_SET2);
    tagSets.add(tSet);
    List<String> satisfy = mongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
    // no replica set members have the "use : ops" tag in their tag sets
    assertEquals(0, satisfy.size());
  }

  @Test
  void testGetReplicaSetMembersThatSatisfyTagSetsThrowsOnDbError() {
    setupMockedReplSet();
    List<Document> tagSets = new ArrayList<>(); // tags to satisfy
    Document tSet = Document.parse(TAG_SET);
    tagSets.add(tSet);
    Mockito.doThrow(runtimeException)
        .when(mockMongoClient)
        .getDatabase(MongoClientWrapperImpl.LOCAL_DB);
    try {
      mongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
      fail("expected exception.");
    } catch (Exception e) {
      assertTrue(e instanceof MongoDbException);
    }
  }

  @Test
  void testGetClientStandalone() throws MongoDbException {
    ServerAddress address = new ServerAddress("fakehost", 1010);
    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn(address.getHost());
    Mockito.when(mongoProperties.get(MongoProp.PORT)).thenReturn("1010");
    Mockito.when(mongoProperties.useAllReplicaSetMembers()).thenReturn(false);

    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
    mongoClientWrapper.getClient(settingsBuilder);

    Mockito.verify(mongoClientFactory)
        .getMongoClient(
            serverAddresses.capture(),
            mongoCredentials.capture(),
            Mockito.any(MongoClientSettings.Builder.class),
            Mockito.eq(false));

    assertEquals(Arrays.asList(address), serverAddresses.getValue());
    assertEquals(0, mongoCredentials.getValue().size());
  }

  @Test
  void testGetClientRepSet() throws MongoDbException {
    Mockito.when(mongoProperties.get(MongoProp.HOST))
        .thenReturn("host1:1010,host2:2020,host3:3030");
    Mockito.when(mongoProperties.useAllReplicaSetMembers()).thenReturn(true);
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
    mongoClientWrapper.getClient(settingsBuilder);

    Mockito.verify(mongoClientFactory)
        .getMongoClient(
            serverAddresses.capture(),
            mongoCredentials.capture(),
            Mockito.any(MongoClientSettings.Builder.class),
            Mockito.eq(true));

    List<ServerAddress> addresses =
        Arrays.asList(
            new ServerAddress("host1", 1010),
            new ServerAddress("host2", 2020),
            new ServerAddress("host3", 3030));
    assertEquals(addresses, serverAddresses.getValue());
  }

  @Test
  void testBadHostsAndPorts() {
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("host1:1010:2020");
    try {
      mongoClientWrapper.getClient(settingsBuilder);
      fail("expected exception:  malformed host ");
    } catch (MongoDbException e) {
      // Do nothing
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("");
    try {
      mongoClientWrapper.getClient(settingsBuilder);
      fail("expected exception:  empty host string ");
    } catch (MongoDbException e) {
      // Do nothing
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("   ");
    try {
      mongoClientWrapper.getClient(settingsBuilder);
      fail("expected exception:  empty host string ");
    } catch (MongoDbException e) {
      // Do nothing
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("host1:badport");
    try {
      mongoClientWrapper.getClient(settingsBuilder);
      fail("expected exception:  bad port ");
    } catch (MongoDbException e) {
      // Do nothing
    }
  }

  @Test
  void testConstructorInitializesClientWithProps() throws MongoDbException {
    final AtomicBoolean clientCalled = new AtomicBoolean(false);
    final MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
    Mockito.when(mongoProperties.buildMongoClientSettings(mockMongoUtilLogger))
        .thenReturn(settingsBuilder);
    new MongoClientWrapperImpl(mongoProperties, mockMongoUtilLogger) {
      @Override
      protected MongoClient getClient(MongoClientSettings.Builder opts) {
        clientCalled.set(true);
        return null;
      }
    };
    Mockito.verify(mongoProperties).buildMongoClientSettings(mockMongoUtilLogger);
    assertTrue(clientCalled.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  void operationsDelegateToMongoClient() throws MongoDbException {
    ListDatabasesIterable<Document> listDbIterable = Mockito.mock(ListDatabasesIterable.class);
    Mockito.when(mockMongoClient.listDatabaseNames())
        .thenReturn(Mockito.mock(com.mongodb.client.MongoIterable.class));
    mongoClientWrapper.getDatabaseNames();
    Mockito.verify(mockMongoClient).listDatabaseNames();

    Mockito.when(mockMongoClient.getDatabase("foo")).thenReturn(mockDB);
    mongoClientWrapper.getDb("foo");
    Mockito.verify(mockMongoClient).getDatabase("foo");

    ListCollectionNamesIterable collNamesIterable = Mockito.mock(ListCollectionNamesIterable.class);
    Mockito.when(mockDB.listCollectionNames()).thenReturn(collNamesIterable);
    mongoClientWrapper.getCollectionsNames("foo");
    Mockito.verify(mockDB).listCollectionNames();
  }

  @Test
  void mongoExceptionsPropogate() {
    Mockito.doThrow(runtimeException).when(mockMongoClient).listDatabaseNames();
    try {
      mongoClientWrapper.getDatabaseNames();
      fail("expected exception");
    } catch (Exception mde) {
      assertTrue(mde instanceof MongoDbException);
    }
    Mockito.doThrow(runtimeException).when(mockMongoClient).getDatabase("foo");
    try {
      mongoClientWrapper.getDb("foo");
      fail("expected exception");
    } catch (Exception mde) {
      assertTrue(mde instanceof MongoDbException);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void mongoGetCollNamesExceptionPropgates() {
    Mockito.when(mockMongoClient.getDatabase("foo")).thenReturn(mockDB);
    Mockito.doThrow(runtimeException).when(mockDB).listCollectionNames();
    try {
      mongoClientWrapper.getCollectionsNames("foo");
      fail("expected exception");
    } catch (Exception mde) {
      assertTrue(mde instanceof MongoDbException);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetIndex() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase("fakeDb")).thenReturn(mockDB);
    ListCollectionNamesIterable collNamesIterable = Mockito.mock(ListCollectionNamesIterable.class);
    MongoCursor<String> collNamesCursor = Mockito.mock(MongoCursor.class);
    Mockito.when(mockDB.listCollectionNames()).thenReturn(collNamesIterable);
    Mockito.when(collNamesIterable.iterator()).thenReturn(collNamesCursor);
    Mockito.when(collNamesCursor.hasNext()).thenReturn(true, false);
    Mockito.when(collNamesCursor.next()).thenReturn("collection");

    collection = Mockito.mock(MongoCollection.class);
    ListIndexesIterable<Document> listIndexesIterable = Mockito.mock(ListIndexesIterable.class);
    MongoCursor<Document> indexCursor = Mockito.mock(MongoCursor.class);
    Document indexDoc = new Document("name", "testIndex");

    Mockito.when(collection.listIndexes()).thenReturn(listIndexesIterable);
    Mockito.when(listIndexesIterable.iterator()).thenReturn(indexCursor);
    Mockito.when(indexCursor.hasNext()).thenReturn(true, false);
    Mockito.when(indexCursor.next()).thenReturn(indexDoc);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);

    List<String> result = mongoClientWrapper.getIndexInfo("fakeDb", "collection");
    assertEquals(1, result.size());
    assertTrue(result.get(0).contains("testIndex"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetIndexCollectionDoesntExist() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase("fakeDb")).thenReturn(mockDB);
    ListCollectionNamesIterable collNamesIterable = Mockito.mock(ListCollectionNamesIterable.class);
    MongoCursor<String> collNamesCursor = Mockito.mock(MongoCursor.class);
    Mockito.when(mockDB.listCollectionNames()).thenReturn(collNamesIterable);
    Mockito.when(collNamesIterable.iterator()).thenReturn(collNamesCursor);
    Mockito.when(collNamesCursor.hasNext()).thenReturn(false);

    collection = Mockito.mock(MongoCollection.class);
    ListIndexesIterable<Document> listIndexesIterable = Mockito.mock(ListIndexesIterable.class);
    MongoCursor<Document> indexCursor = Mockito.mock(MongoCursor.class);
    Document indexDoc = new Document("name", "testIndex");

    Mockito.when(collection.listIndexes()).thenReturn(listIndexesIterable);
    Mockito.when(listIndexesIterable.iterator()).thenReturn(indexCursor);
    Mockito.when(indexCursor.hasNext()).thenReturn(true, false);
    Mockito.when(indexCursor.next()).thenReturn(indexDoc);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);

    List<String> result = mongoClientWrapper.getIndexInfo("fakeDb", "collection");
    assertEquals(1, result.size());
    Mockito.verify(mockDB).createCollection("collection");
  }

  @Test
  void testGetIndexCollectionNotSpecified() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase("fakeDb")).thenReturn(mockDB);
    try {
      mongoClientWrapper.getIndexInfo("fakeDb", "");
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof MongoDbException);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void getIndexInfoErrorConditions() {
    try {
      mongoClientWrapper.getIndexInfo("fakeDb", "collection");
      fail("expected exception since DB is null");
    } catch (Exception e) {
      // Do nothing
    }
    try {
      mongoClientWrapper.getIndexInfo("fakeDb", "");
      fail("expected exception since no collection specified.");
    } catch (Exception e) {
      // Do nothing
    }
    Mockito.when(mockMongoClient.getDatabase("fakeDb")).thenReturn(mockDB);
    ListCollectionNamesIterable collNamesIterable = Mockito.mock(ListCollectionNamesIterable.class);
    MongoCursor<String> collNamesCursor = Mockito.mock(MongoCursor.class);
    Mockito.when(mockDB.listCollectionNames()).thenReturn(collNamesIterable);
    Mockito.when(collNamesIterable.iterator()).thenReturn(collNamesCursor);
    Mockito.when(collNamesCursor.hasNext()).thenReturn(true, false);
    Mockito.when(collNamesCursor.next()).thenReturn("collection");
    try {
      mongoClientWrapper.getIndexInfo("fakeDb", "collection");
      fail("expected exception since null collection");
    } catch (Exception e) {
      Mockito.verify(mockDB).getCollection("collection");
      assertTrue(
          e.getMessage()
              .contains(
                  BaseMessages.getString(
                      PKG,
                      "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection",
                      "collection")));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetIndexNoIndexThrows() {
    initFakeDbWithEmptyIndexes();
    try {
      mongoClientWrapper.getIndexInfo("fakeDb", "collection");
      fail("expected exception since no index info");
    } catch (Exception e) {
      Mockito.verify(mockDB).getCollection("collection");
      assertTrue(
          e.getMessage()
              .contains(
                  BaseMessages.getString(
                      PKG,
                      "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection",
                      "collection")));
    }
  }

  @SuppressWarnings("unchecked")
  private void initFakeDb() {
    Mockito.when(mockMongoClient.getDatabase("fakeDb")).thenReturn(mockDB);
    ListCollectionNamesIterable collNamesIterable = Mockito.mock(ListCollectionNamesIterable.class);
    MongoCursor<String> collNamesCursor = Mockito.mock(MongoCursor.class);
    Mockito.when(mockDB.listCollectionNames()).thenReturn(collNamesIterable);
    Mockito.when(collNamesIterable.iterator()).thenReturn(collNamesCursor);
    Mockito.when(collNamesCursor.hasNext()).thenReturn(true, false);
    Mockito.when(collNamesCursor.next()).thenReturn("collection");
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);
  }

  @SuppressWarnings("unchecked")
  private void initFakeDbWithEmptyIndexes() {
    initFakeDb();
    // Set up collection to return an empty list of indexes
    ListIndexesIterable<Document> listIndexesIterable = Mockito.mock(ListIndexesIterable.class);
    MongoCursor<Document> indexCursor = Mockito.mock(MongoCursor.class);
    Mockito.when(collection.listIndexes()).thenReturn(listIndexesIterable);
    Mockito.when(listIndexesIterable.iterator()).thenReturn(indexCursor);
    Mockito.when(indexCursor.hasNext()).thenReturn(false); // No indexes
  }

  @Test
  void testGetAllTagsNoDB() throws MongoDbException {
    List<String> tags = mongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable"));
    assertEquals(0, tags.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetAllTagsNoRepSet() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase(MongoClientWrapperImpl.LOCAL_DB)).thenReturn(mockDB);
    List<String> tags = mongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable"));
    assertEquals(0, tags.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetAllTagsRepSetEmpty() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase(MongoClientWrapperImpl.LOCAL_DB)).thenReturn(mockDB);
    MongoCollection<Document> replSetCollection = Mockito.mock(MongoCollection.class);
    FindIterable<Document> findIterable = Mockito.mock(FindIterable.class);
    Mockito.when(mockDB.getCollection(MongoClientWrapperImpl.REPL_SET_COLLECTION))
        .thenReturn(replSetCollection);
    List<Document> membersList = new ArrayList<>();
    Document configDoc = new Document(MongoClientWrapperImpl.REPL_SET_MEMBERS, membersList);
    Mockito.when(replSetCollection.find()).thenReturn(findIterable);
    Mockito.when(findIterable.first()).thenReturn(configDoc);
    List<String> tags = mongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    assertEquals(0, tags.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetAllTagsRepSetNull() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase(MongoClientWrapperImpl.LOCAL_DB)).thenReturn(mockDB);
    MongoCollection<Document> replSetCollection = Mockito.mock(MongoCollection.class);
    FindIterable<Document> findIterable = Mockito.mock(FindIterable.class);
    Mockito.when(mockDB.getCollection(MongoClientWrapperImpl.REPL_SET_COLLECTION))
        .thenReturn(replSetCollection);
    Mockito.when(replSetCollection.find()).thenReturn(findIterable);
    Mockito.when(findIterable.first()).thenReturn(null);
    List<String> tags = mongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    assertEquals(0, tags.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetAllTagsRepSetUnexpectedType() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase(MongoClientWrapperImpl.LOCAL_DB)).thenReturn(mockDB);
    MongoCollection<Document> replSetCollection = Mockito.mock(MongoCollection.class);
    FindIterable<Document> findIterable = Mockito.mock(FindIterable.class);
    Mockito.when(mockDB.getCollection(MongoClientWrapperImpl.REPL_SET_COLLECTION))
        .thenReturn(replSetCollection);
    Document configDoc = new Document(MongoClientWrapperImpl.REPL_SET_MEMBERS, "not a list");
    Mockito.when(replSetCollection.find()).thenReturn(findIterable);
    Mockito.when(findIterable.first()).thenReturn(configDoc);
    List<String> tags = mongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    assertEquals(0, tags.size());
  }

  @Test
  void testGetAllTags() throws MongoDbException {
    setupMockedReplSet();
    List<String> tags = mongoClientWrapper.getAllTags();
    Collections.sort(tags, String.CASE_INSENSITIVE_ORDER);
    assertEquals(
        Arrays.asList(
            "\"dc.one\" : \"primary\"",
            "\"dc.three\" : \"slave2\"",
            "\"dc.two\" : \"slave1\"",
            "\"use\" : \"production\""),
        tags);
  }

  @Test
  void testGetCreateCollection() throws MongoDbException {
    Mockito.when(mockMongoClient.getDatabase("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);
    mongoClientWrapper.getCollection("fakeDb", "collection");
    Mockito.verify(mockDB).getCollection("collection");
    mongoClientWrapper.createCollection("fakeDb", "newCollection");
    Mockito.verify(mockDB).createCollection("newCollection");
  }

  @Test
  void testClientDelegation() {
    mongoClientWrapper.dispose();
    Mockito.verify(mockMongoClient).close();
  }

  @SuppressWarnings("unchecked")
  private void setupMockedReplSet() {
    Mockito.when(mockMongoClient.getDatabase(MongoClientWrapperImpl.LOCAL_DB)).thenReturn(mockDB);
    MongoCollection<Document> replSetCollection = Mockito.mock(MongoCollection.class);
    FindIterable<Document> findIterable = Mockito.mock(FindIterable.class);
    Mockito.when(mockDB.getCollection(MongoClientWrapperImpl.REPL_SET_COLLECTION))
        .thenReturn(replSetCollection);
    Document config = Document.parse(REP_SET_CONFIG);
    Object members = config.get(MongoClientWrapperImpl.REPL_SET_MEMBERS);
    Document configDoc = new Document(MongoClientWrapperImpl.REPL_SET_MEMBERS, members);
    Mockito.when(replSetCollection.find()).thenReturn(findIterable);
    Mockito.when(findIterable.first()).thenReturn(configDoc);
  }
}
