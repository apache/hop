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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class NoAuthMongoClientWrapperTest {

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
  @Mock private MongoClientOptions mongoClientOptions;
  @Mock private DB mockDB;
  @Mock DBCollection collection;
  @Mock private RuntimeException runtimeException;
  @Captor private ArgumentCaptor<List<ServerAddress>> serverAddresses;
  @Captor private ArgumentCaptor<List<MongoCredential>> mongoCredentials;

  private NoAuthMongoClientWrapper noAuthMongoClientWrapper;

  private static final Class<?> PKG = NoAuthMongoClientWrapper.class;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(
            mongoClientFactory.getMongoClient(
                Mockito.anyList(),
                Mockito.anyList(),
                Mockito.any(MongoClientOptions.class),
                Mockito.anyBoolean()))
        .thenReturn(mockMongoClient);
    NoAuthMongoClientWrapper.clientFactory = mongoClientFactory;
    noAuthMongoClientWrapper =
        new NoAuthMongoClientWrapper(mockMongoClient, mongoProperties, mockMongoUtilLogger);
  }

  @Test
  void testPerform() throws Exception {
    MongoDBAction mockMongoDBAction = Mockito.mock(MongoDBAction.class);
    noAuthMongoClientWrapper.perform("Test", mockMongoDBAction);
    Mockito.verify(mockMongoDBAction, Mockito.times(1))
        .perform(noAuthMongoClientWrapper.getDb("Test"));
  }

  @Test
  void testGetLastErrorMode() throws MongoDbException {
    DBObject config = (DBObject) BasicDBObject.parse(REP_SET_CONFIG);
    DBCollection dbCollection = Mockito.mock(DBCollection.class);
    Mockito.when(dbCollection.findOne()).thenReturn(config);
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(dbCollection);

    assertEquals(Arrays.asList("DCThree"), noAuthMongoClientWrapper.getLastErrorModes());
  }

  @Test
  void testGetAllReplicaSetMemberRecords() {
    DBObject config = (DBObject) BasicDBObject.parse(REP_SET_CONFIG);
    Object members = config.get(NoAuthMongoClientWrapper.REPL_SET_MEMBERS);

    assertNotNull(members);
    assertTrue(members instanceof BasicDBList);
    assertEquals(3, ((BasicDBList) members).size());
  }

  @Test
  void testSetupAllTags() {
    DBObject config = (DBObject) BasicDBObject.parse(REP_SET_CONFIG);
    Object members = config.get(NoAuthMongoClientWrapper.REPL_SET_MEMBERS);

    List<String> allTags = noAuthMongoClientWrapper.setupAllTags((BasicDBList) members);

    assertEquals(4, allTags.size());
  }

  @Test
  void testGetReplicaSetMembersThatSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();

    List<DBObject> tagSets = new ArrayList<>(); // tags to satisfy

    DBObject tSet = (DBObject) BasicDBObject.parse(TAG_SET);
    tagSets.add(tSet);

    List<String> satisfy = noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
    // two replica set members have the "use : production" tag in their tag sets
    assertEquals(2, satisfy.size());
    assertTrue(satisfy.get(0).contains("palladium.lan:27017"));
    assertTrue(satisfy.get(1).contains("palladium.local:27019"));
  }

  @Test
  void testGetReplicaSetMembersBadInput() {
    setupMockedReplSet();
    try {
      noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(null);
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof MongoDbException);
    }
  }

  @Test
  void testGetReplicaSetMembersDoesntSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<>(); // tags to satisfy
    DBObject tSet = (DBObject) BasicDBObject.parse(TAG_SET2);
    tagSets.add(tSet);
    List<String> satisfy = noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
    // no replica set members have the "use : ops" tag in their tag sets
    assertEquals(0, satisfy.size());
  }

  @Test
  void testGetReplicaSetMembersThatSatisfyTagSetsThrowsOnDbError() {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<>(); // tags to satisfy
    DBObject tSet = (DBObject) BasicDBObject.parse(TAG_SET);
    tagSets.add(tSet);
    Mockito.doThrow(runtimeException)
        .when(mockMongoClient)
        .getDB(NoAuthMongoClientWrapper.LOCAL_DB);
    try {
      noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
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

    noAuthMongoClientWrapper.getClient(mongoClientOptions);

    Mockito.verify(mongoClientFactory)
        .getMongoClient(
            serverAddresses.capture(),
            mongoCredentials.capture(),
            Mockito.any(MongoClientOptions.class),
            Mockito.eq(false));

    assertEquals(Arrays.asList(address), serverAddresses.getValue());
    assertEquals(0, mongoCredentials.getValue().size());
  }

  @Test
  void testGetClientRepSet() throws MongoDbException {
    Mockito.when(mongoProperties.get(MongoProp.HOST))
        .thenReturn("host1:1010,host2:2020,host3:3030");
    Mockito.when(mongoProperties.useAllReplicaSetMembers()).thenReturn(true);
    noAuthMongoClientWrapper.getClient(mongoClientOptions);

    Mockito.verify(mongoClientFactory)
        .getMongoClient(
            serverAddresses.capture(),
            mongoCredentials.capture(),
            Mockito.eq(mongoClientOptions),
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
    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("host1:1010:2020");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      fail("expected exception:  malformed host ");
    } catch (MongoDbException e) {
      // Do nothing
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      fail("expected exception:  empty host string ");
    } catch (MongoDbException e) {
      // Do nothing
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("   ");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      fail("expected exception:  empty host string ");
    } catch (MongoDbException e) {
      // Do nothing
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("host1:badport");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      fail("expected exception:  bad port ");
    } catch (MongoDbException e) {
      // Do nothing
    }
  }

  @Test
  void testConstructorInitializesClientWithProps() throws MongoDbException {
    final AtomicBoolean clientCalled = new AtomicBoolean(false);
    final MongoClientOptions options = Mockito.mock(MongoClientOptions.class);
    Mockito.when(mongoProperties.buildMongoClientOptions(mockMongoUtilLogger)).thenReturn(options);
    new NoAuthMongoClientWrapper(mongoProperties, mockMongoUtilLogger) {
      @Override
      protected MongoClient getClient(MongoClientOptions opts) {
        clientCalled.set(true);
        assertEquals(options, opts);
        return null;
      }
    };
    Mockito.verify(mongoProperties).buildMongoClientOptions(mockMongoUtilLogger);
    assertTrue(clientCalled.get());
  }

  @Test
  void operationsDelegateToMongoClient() throws MongoDbException {
    noAuthMongoClientWrapper.getDatabaseNames();
    Mockito.verify(mockMongoClient).getDatabaseNames();

    noAuthMongoClientWrapper.getDb("foo");
    Mockito.verify(mockMongoClient).getDB("foo");

    Mockito.when(mockMongoClient.getDB("foo")).thenReturn(mockDB);
    noAuthMongoClientWrapper.getCollectionsNames("foo");
    Mockito.verify(mockDB).getCollectionNames();
  }

  @Test
  void mongoExceptionsPropogate() {
    Mockito.doThrow(runtimeException).when(mockMongoClient).getDatabaseNames();
    try {
      noAuthMongoClientWrapper.getDatabaseNames();
      fail("expected exception");
    } catch (Exception mde) {
      assertTrue(mde instanceof MongoDbException);
    }
    Mockito.doThrow(runtimeException).when(mockMongoClient).getDB("foo");
    try {
      noAuthMongoClientWrapper.getDb("foo");
      fail("expected exception");
    } catch (Exception mde) {
      assertTrue(mde instanceof MongoDbException);
    }
  }

  @Test
  void mongoGetCollNamesExceptionPropgates() {
    Mockito.when(mockMongoClient.getDB("foo")).thenReturn(mockDB);
    Mockito.doThrow(runtimeException).when(mockDB).getCollectionNames();
    try {
      noAuthMongoClientWrapper.getCollectionsNames("foo");
      fail("expected exception");
    } catch (Exception mde) {
      assertTrue(mde instanceof MongoDbException);
    }
  }

  @Test
  void testGetIndex() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(true);
    collection = Mockito.mock(DBCollection.class);
    DBObject indexInfoObj = Mockito.mock(DBObject.class);
    Mockito.when(indexInfoObj.toString()).thenReturn("indexInfo");
    List<DBObject> indexInfo = Arrays.asList(indexInfoObj);
    Mockito.when(collection.getIndexInfo()).thenReturn(indexInfo);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);

    assertEquals(
        Arrays.asList("indexInfo"), noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection"));
  }

  @Test
  void testGetIndexCollectionDoesntExist() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(false);
    collection = Mockito.mock(DBCollection.class);
    DBObject indexInfoObj = Mockito.mock(DBObject.class);
    Mockito.when(indexInfoObj.toString()).thenReturn("indexInfo");
    List<DBObject> indexInfo = Arrays.asList(indexInfoObj);
    Mockito.when(collection.getIndexInfo()).thenReturn(indexInfo);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);
    assertEquals(
        Arrays.asList("indexInfo"), noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection"));
    Mockito.verify(mockDB).createCollection("collection", null);
  }

  @Test
  void testGetIndexCollectionNotSpecified() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "");
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof MongoDbException);
    }
  }

  @Test
  void getIndexInfoErrorConditions() {
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection");
      fail("expected exception since DB is null");
    } catch (Exception e) {
      // Do nothing
    }
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "");
      fail("expected exception since no collection specified.");
    } catch (Exception e) {
      // Do nothing
    }
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(true);
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection");
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
  void testGetIndexNoIndexThrows() {
    initFakeDb();
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection");
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

  private void initFakeDb() {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(true);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);
  }

  @Test
  void testGetAllTagsNoDB() throws MongoDbException {
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable"));
    assertEquals(0, tags.size());
  }

  @Test
  void testGetAllTagsNoRepSet() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable"));
    assertEquals(0, tags.size());
  }

  @Test
  void testGetAllTagsRepSetEmtpy() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    DBObject membersList = new BasicDBList();
    DBObject basicDBObject =
        new BasicDBObject(NoAuthMongoClientWrapper.REPL_SET_MEMBERS, membersList);
    Mockito.when(collection.findOne()).thenReturn(basicDBObject);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    assertEquals(0, tags.size());
  }

  @Test
  void testGetAllTagsRepSetNull() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    Mockito.when(collection.findOne()).thenReturn(null);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    assertEquals(0, tags.size());
  }

  @Test
  void testGetAllTagsRepSetUnexpectedType() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    DBObject dbObj = Mockito.mock(DBObject.class);
    Mockito.when(collection.findOne()).thenReturn(dbObj);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    assertEquals(0, tags.size());
  }

  @Test
  void testGetAllTags() throws MongoDbException {
    setupMockedReplSet();
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
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
    initFakeDb();
    noAuthMongoClientWrapper.getCollection("fakeDb", "collection");
    Mockito.verify(mockDB).getCollection("collection");
    noAuthMongoClientWrapper.createCollection("fakeDb", "newCollection");
    Mockito.verify(mockDB).createCollection("newCollection", null);
  }

  @Test
  void testClientDelegation() {
    noAuthMongoClientWrapper.dispose();
    Mockito.verify(mockMongoClient).close();
    noAuthMongoClientWrapper.getReplicaSetStatus();
    Mockito.verify(mockMongoClient).getReplicaSetStatus();
  }

  private void setupMockedReplSet() {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    DBObject config = (DBObject) BasicDBObject.parse(REP_SET_CONFIG);
    Object members = config.get(NoAuthMongoClientWrapper.REPL_SET_MEMBERS);
    DBObject basicDBObject = new BasicDBObject(NoAuthMongoClientWrapper.REPL_SET_MEMBERS, members);
    Mockito.when(collection.findOne()).thenReturn(basicDBObject);
  }
}
